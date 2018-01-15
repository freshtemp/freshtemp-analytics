<?php


date_default_timezone_set('UTC');

$start = microtime(true);

// define BASEPATH so we can load the necessary config entries, then load it
define('BASEPATH', pathinfo(__FILE__, PATHINFO_DIRNAME));

$dbSlave = new PDO('mysql:host=172.21.1.55;dbname=tempmon;charset=utf8', 'root', 'D1g1Fr3shT3mp');
$dbSlave->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

$dbMaster = new PDO('mysql:host=172.21.2.20;dbname=tempmon;charset=utf8', 'root', 'D1g1Fr3shT3mp');
$dbMaster->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);


//Only check for tasks marked null.
$sth = $dbMaster->prepare("SELECT id,timezone,day_week_starts FROM organization WHERE compute_analytics = 'Y'");

$sth->execute();

while ($row = $sth->fetch()) {
	$OrganizationId = $row['id'];
	
	$Locations = getLocationsFromOrganization($dbMaster, $OrganizationId);

	$reportWeeks = getReportWeeksFromOrganization($row['timezone'], $row['day_week_starts']);

	foreach($Locations AS $Location) {

      		echo "Gathering metrics for location: " . $Location->location . "\n";

      		calculateLocationTaskMetrics($dbSlave, $dbMaster, $OrganizationId, $Location->location, $reportWeeks);

      		calculateLocationTaskGroupMetrics($dbSlave, $dbMaster, $Location, $reportWeeks);
  	}

	echo "\n";
}

echo "\n";

$time_elapsed_secs = microtime(true) - $start;

echo 'Total Execution Time: ' . $time_elapsed_secs . ' Seconds';

echo "\n";

function getLocationsFromOrganization($db, $OrganizationId) {

	//We need to determine what tasks should be completed and what tasks have not been completed.
	$r = $db->prepare("SELECT location.id AS location,location.timezone FROM organization_location LEFT JOIN location ON location.id = organization_location.location WHERE organization = ?");

	$r->execute(array($OrganizationId));

	return $r->fetchAll(PDO::FETCH_OBJ);

}

function getReportWeeksFromOrganization($timezone, $dayWeekStarts) {

  $weeks = 6;

  echo "Day Week Starts: " . $dayWeekStarts;
  echo "\n";

  $reportWeeks = [];

  //First get time at location
  $timeAtLocation = new DateTime("now", new DateTimeZone($timezone));

  //Get the Day Of Week as Integer
  $currentDayAtLocation = $timeAtLocation->format('N');

  if($currentDayAtLocation == $dayWeekStarts) {

    $mostRecentReportEnded = $timeAtLocation->sub(new DateInterval('P1D'));

  } else if($currentDayAtLocation > $dayWeekStarts) {

    $daysLastReportEnded = $currentDayAtLocation - $dayWeekStarts + 1;

    $mostRecentReportEnded = $timeAtLocation->sub(new DateInterval('P' . $daysLastReportEnded . 'D'));

  } else {

    $daysLastReportEnded = 7 - ($dayWeekStarts - $currentDayAtLocation) + 1;

    $mostRecentReportEnded = $timeAtLocation->sub(new DateInterval('P' . $daysLastReportEnded . 'D'));

  }

  $mostRecentReportStarted = clone $mostRecentReportEnded;
    
  $mostRecentReportStarted->sub(new DateInterval('P6D'));

  for($i = 0; $i < $weeks; $i++) {
    $reportWeek = new stdClass();


    $reportWeek->start_day  = clone $mostRecentReportStarted;
    $reportWeek->end_day    = clone $mostRecentReportEnded;

    $reportWeek->end_day_formatted    = $mostRecentReportEnded->format("M jS");
    $reportWeek->start_day_formatted  = $mostRecentReportStarted->format("M jS");

    $mostRecentReportEnded->sub(new DateInterval('P7D'));
    $mostRecentReportStarted->sub(new DateInterval('P7D'));
    array_push($reportWeeks, $reportWeek);
  }

  return $reportWeeks;

}

function calculateLocationTaskGroupMetrics($dbSlave, $dbMaster, $Location, $reportWeeks) {

  foreach($reportWeeks AS $reportWeek) {

    $reportWeekEndDay = $reportWeek->end_day->format("Y-m-d");

    $TaskBlocks = get_task_blocks_from_location_with_completed_task_for_this_week($dbSlave, $Location->location, $reportWeek);

    if($TaskBlocks && count($TaskBlocks) > 0) {

      foreach($TaskBlocks AS $TaskBlock) {

        $data_is_valid = false;

        $q1 = $dbSlave->prepare("SELECT MIN(marked_at) AS start_time, MAX(marked_at) AS stop_time, count(DISTINCT(task)) AS total FROM task_result WHERE completed = 'Y' AND task_block = ? AND day BETWEEN ? AND ? GROUP BY task_block,day");

        $q1->execute(array($TaskBlock->task_block, $reportWeek->start_day->format("Y-m-d"),$reportWeek->end_day->format("Y-m-d")));

        $startTimeArr = [];
        $stopTimeArr = [];

        $Results = $q1->fetchAll(PDO::FETCH_OBJ);

        foreach($Results AS $r) {

          if($r->total > 2) {
            if(!empty($r->start_time)) {
              $start_time_in_seconds_from_midnight = utc_to_local($r->start_time, $Location->timezone, "H") * 60 + utc_to_local($r->start_time, $Location->timezone, "i") + utc_to_local($r->start_time, $Location->timezone, "s");

              
            }

            if(!empty($r->stop_time)) {
              $stop_time_in_seconds_from_midnight = utc_to_local($r->stop_time, $Location->timezone, "H") * 60 * 60 + utc_to_local($r->stop_time, $Location->timezone, "i") * 60 + utc_to_local($r->stop_time, $Location->timezone, "s");


            }

            if($start_time_in_seconds_from_midnight && $stop_time_in_seconds_from_midnight) {
              $time_to_complete = get_timespan_between_two_datetimes($r->start_time, $r->stop_time);

              if($time_to_complete < 3600) {
                //Include!
                $startTimeArr[] = $start_time_in_seconds_from_midnight;
                $stopTimeArr[] = $stop_time_in_seconds_from_midnight;
              }
            }
          }
        }

        if(count($startTimeArr) > 1 && count($stopTimeArr) > 1) {

          $averageStartTime = array_sum($startTimeArr) / count($startTimeArr);
          $averageStopTime = array_sum($stopTimeArr) / count($stopTimeArr);

          $startTimeVolatility = stats_standard_deviation($startTimeArr);
          $stopTimeVolatility = stats_standard_deviation($stopTimeArr);

          $q3 = $dbMaster->prepare("INSERT INTO weekly_location_task_group_metrics (location,task_group,week_end_day,average_start_time_seconds_from_midnight,average_start_time_volatility,average_stop_time_seconds_from_midnight,average_stop_time_volatility) VALUES (?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE average_start_time_seconds_from_midnight = ?, average_start_time_volatility = ?, average_stop_time_seconds_from_midnight = ?, average_stop_time_volatility = ?");

          $q3->execute(array($Location->location, $TaskBlock->task_block, $reportWeekEndDay, $averageStartTime, $startTimeVolatility, $averageStopTime, $stopTimeVolatility, $averageStartTime, $startTimeVolatility, $averageStopTime, $stopTimeVolatility));
        }
      }
    }

  }

}

function calculateLocationTaskMetrics($dbSlave, $dbMaster, $organizationId, $locationId, $reportWeeks) {
  foreach($reportWeeks AS $reportWeek) {

    $reportWeekEndDay = $reportWeek->end_day->format("Y-m-d");

    echo $reportWeekEndDay;
    echo "\n";

    $Results = get_summary_from_single_location_between_two_dates($dbSlave, $locationId, $reportWeek->start_day, $reportWeek->end_day);

    $ViolationObj = get_unresolved_violations_from_location_between_two_dates($dbSlave, $locationId, $reportWeek->start_day, $reportWeek->end_day);

    $locationsWithCompletedTask = 0;

    $completionPercentage = 0;

    $totalCompletedPlusMissed = $Results->completed_scheduled + $Results->missed;

    if($totalCompletedPlusMissed > 0) {
      $completionPercentage = $Results->completed_scheduled / $totalCompletedPlusMissed * 100;

      //We should be smarter here...  Or triggering an event to handle.
      if($completionPercentage > 100) {
        $completionPercentage = 100;
      }

    }

    $r = $dbMaster->prepare("INSERT INTO weekly_location_task_metrics (organization, location,week_end_day,completed_tasks,missed_tasks,violations, violations_resolved, violation_follow_ups_required, completion_percentage) VALUES (?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE completed_tasks = ?, missed_tasks = ?, violations = ?, violations_resolved = ?, violation_follow_ups_required = ?, completion_percentage = ?");

    $r->execute(array($organizationId,$locationId, $reportWeekEndDay, $Results->completed_scheduled, $Results->missed, $Results->violations, $ViolationObj->resolvedViolations, $ViolationObj->followUpsRequired, $completionPercentage, $Results->completed_scheduled, $Results->missed, $Results->violations, $ViolationObj->resolvedViolations, $ViolationObj->followUpsRequired, $completionPercentage));
  }
}

function get_task_blocks_from_location_with_completed_task_for_this_week($db, $locationId, $reportWeek) {

  $r = $db->prepare("SELECT DISTINCT(task_block) AS task_block FROM task_result WHERE day BETWEEN ? AND ? AND location = ? AND completed = 'Y'");

  $r->execute(array($reportWeek->start_day->format("Y-m-d"), $reportWeek->end_day->format("Y-m-d"), $locationId));

  return $r->fetchAll(PDO::FETCH_OBJ);
}

function get_summary_from_single_location_between_two_dates($db, $locationId, $dateFrom, $dateTo) {
  //Run the magical query
  $r = $db->prepare("SELECT count(R.id) total, sum(case when R.completed = 'Y' AND R.task_block IS NOT NULL then 1 else 0 end) completed_scheduled, sum(case when R.completed = 'N' then 1 else 0 end) missed, sum(case when R.completed = 'NA' then 1 else 0 end) notavailable, sum(case when R.corrective_action IS NOT NULL AND completed = 'Y' then 1 else 0 end) violations, sum(case when R.completed = 'Y' AND R.task_block IS NULL then 1 else 0 end) completed_not_scheduled from task_result R WHERE R.location = ? AND R.day BETWEEN ? AND ? ORDER BY violations DESC;");

  $r->execute(array($locationId, $dateFrom->format("Y-m-d"),$dateTo->format("Y-m-d")));

	return $r->fetch(PDO::FETCH_OBJ);
}

function get_unresolved_violations_from_location_between_two_dates($db, $locationId, $dateFrom, $dateTo) {

  $ViolationObj = new stdClass();

  $resolvedViolations = 0;

  $followUpsRequired = 0;

  $CorrectiveActions = getLocationCorrectiveActions($db, $locationId);

  $r = $db->prepare("SELECT task_result.id AS taskResultID,task_result.result, task.id AS taskID, task_result.day, task.title, task_result.corrective_action AS description, task_result.marked_at, task_result.task_block AS taskBlockID FROM task_result LEFT JOIN task ON task.id = task_result.task WHERE task_result.location = ? AND task_result.completed = 'Y' AND task_result.corrective_action IS NOT NULL AND task_result.day BETWEEN ? AND ? ORDER BY task_result.id DESC");

  $r->execute(array($locationId, $dateFrom->format("Y-m-d"), $dateTo->format("Y-m-d")));

  $Violations = $r->fetchAll(PDO::FETCH_OBJ);

  foreach($Violations AS $Violation) {

    $Violation->requires_follow_up = false;

    $CorrectiveActions = get_corrective_actions_from_task_result_corrective_action($locationId, $Violation->description, $CorrectiveActions);

    foreach($CorrectiveActions AS $CorrectiveAction) {

      if($CorrectiveAction->on_upload == "create-task") {

        $followUpsRequired++;

        $r2 = $db->prepare("SELECT task_result.id AS taskResultID, task_result.result, task.id AS taskID, task.title, task_result.corrective_action AS description, task_result.marked_at FROM task_result LEFT JOIN task ON task.id = task_result.task WHERE task_result.location = ? AND task_result.completed = 'Y' AND task_result.task = ? AND task_result.task_block = ? AND  task_result.day = ? AND task_result.id > ? LIMIT 1");

        $r2->execute(array($locationId, $Violation->taskID,$Violation->taskBlockID, $Violation->day, $Violation->taskResultID));

        if($r2->fetchColumn() > 0) {
          $resolvedViolations++;
        }
      }
    }
  }

  $ViolationObj->resolvedViolations = $resolvedViolations;
  $ViolationObj->followUpsRequired = $followUpsRequired;

  return $ViolationObj;
}

function getLocationCorrectiveActions($db, $locationId) {
	$r = $db->prepare("SELECT * FROM corrective_actions WHERE location = ?");

	$r->execute(array($locationId));

	return $r->fetchAll(PDO::FETCH_OBJ);
}

function get_corrective_actions_from_task_result_corrective_action($locationId, $corrective_action_task_result_string, $CorrectiveActions) {

  $Actions = array();

  $action_strings = explode("|", $corrective_action_task_result_string);

  foreach($action_strings AS $action_string) {

    $CorrectiveAction = get_corrective_action_from_corrective_actions($CorrectiveActions, trim($action_string));

    if($CorrectiveAction) {
      $Actions[] = $CorrectiveAction;
    }
  }

  return $Actions;
}


function get_corrective_action_from_corrective_actions($CorrectiveActions, $name) {

	foreach($CorrectiveActions AS $Action) {
		if($Action->name == $name) {
			return $Action;
		}
	}
}

function utc_to_local($date, $timezone, $format) {
  if($date) {
    $Date = new DateTime($date, new DateTimeZone('UTC'));

    $Date->setTimezone(new DateTimeZone($timezone));

    return $Date->format($format);
  } else {
    return "-";
  }
  
}

function get_timespan_between_two_datetimes($date1, $date2) {

  $to_time = strtotime($date2);

  $from_time = strtotime($date1);

  return round(abs($to_time - $from_time),2,PHP_ROUND_HALF_UP);
}

function stats_standard_deviation(array $a, $sample = false) {
    $n = count($a);
    if ($n === 0) {
        trigger_error("The array has zero elements", E_USER_WARNING);
        return false;
    }
    if ($sample && $n === 1) {
        trigger_error("The array has only 1 element", E_USER_WARNING);
        return false;
    }
    $mean = array_sum($a) / $n;
    $carry = 0.0;
    foreach ($a as $val) {
        $d = ((double) $val) - $mean;
        $carry += $d * $d;
    };
    if ($sample) {
       --$n;
    }
    return sqrt($carry / $n);
}
