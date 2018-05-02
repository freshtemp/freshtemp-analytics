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
$sth = $dbMaster->prepare("SELECT id,name,timezone,day_week_starts FROM organization WHERE compute_analytics = 'Y'");

$sth->execute();

while ($row = $sth->fetch()) {

  $OrganizationName = $row['name'];

  $OrganizationId = $row['id'];

  echo $OrganizationName . "[" . $OrganizationId . "]\n";

  $generateReportForWeekEndingDay = getCurrentReportEndDay($row['timezone']);

  $nextReportWeekEndDay = getNextReportWeekEndDay($row['timezone'],$row['day_week_starts']);

	$Locations = getLocationsFromOrganization($dbSlave, $OrganizationId);

  //Computer analytics for each location
	foreach($Locations AS $Location) {

    echo $Location->name . "\n";

    if($Location->MaxWeekEndDay) {

      $numberOfDaysBetweenLastAndNextReports = getNumberOfDaysBetweenTwoDays($Location->MaxWeekEndDay, $nextReportWeekEndDay);

      //This might not be needed but exists anyway...  number of days should never be greater than 7.
      if($numberOfDaysBetweenLastAndNextReports > 7) {

        generateLocationMetrics($dbSlave, $dbMaster, $Location, $nextReportWeekEndDay, 6);

      } else {

        $numberOfDaysBetweenTodayAndNextReport = getNumberOfDaysBetweenTwoDays($generateReportForWeekEndingDay, $nextReportWeekEndDay);

        if($numberOfDaysBetweenTodayAndNextReport == 0 && $Location->MaxWeekEndDay != $nextReportWeekEndDay) {

          generateLocationMetrics($dbSlave, $dbMaster, $Location, $generateReportForWeekEndingDay, 1);

        }
      }

    } else {

      generateLocationMetrics($dbSlave, $dbMaster, $Location, $nextReportWeekEndDay, 6);

    }

  }

  $Divisions = getDivisionsFromOrganization($dbSlave, $OrganizationId);

  foreach($Divisions AS $Division) {

    if($Division->MaxWeekEndDay) {

      $numberOfDaysBetweenLastAndNextReports = getNumberOfDaysBetweenTwoDays($Division->MaxWeekEndDay, $nextReportWeekEndDay);

      if($numberOfDaysBetweenLastAndNextReports > 7) {

        generateDivisionMetrics($dbSlave, $dbMaster, $Division, $nextReportWeekEndDay, 6);

      } else {

        //Same as determining if they are equal to each other...
        $numberOfDaysBetweenTodayAndNextReport = getNumberOfDaysBetweenTwoDays($generateReportForWeekEndingDay, $nextReportWeekEndDay);

        if($numberOfDaysBetweenTodayAndNextReport == 0 && $Division->MaxWeekEndDay != $nextReportWeekEndDay) {

          generateDivisionMetrics($dbSlave, $dbMaster, $Division, $generateReportForWeekEndingDay, 1);

        }
      }
    } else {
      generateDivisionMetrics($dbSlave, $dbMaster, $Division, $nextReportWeekEndDay, 6);
    }
  }

  $Districts = getDistrictsFromOrganization($dbSlave, $OrganizationId);

  foreach($Districts AS $District) {

    if($District->MaxWeekEndDay) {

      $numberOfDaysBetweenLastAndNextReports = getNumberOfDaysBetweenTwoDays($District->MaxWeekEndDay, $nextReportWeekEndDay);

      if($numberOfDaysBetweenLastAndNextReports > 7) {

        generateDistrictMetrics($dbSlave, $dbMaster, $District, $nextReportWeekEndDay, 6);

      } else {

        //Same as determining if they are equal to each other...
        $numberOfDaysBetweenTodayAndNextReport = getNumberOfDaysBetweenTwoDays($generateReportForWeekEndingDay, $nextReportWeekEndDay);

        if($numberOfDaysBetweenTodayAndNextReport == 0 && $District->MaxWeekEndDay != $nextReportWeekEndDay) {

          generateDistrictMetrics($dbSlave, $dbMaster, $District, $generateReportForWeekEndingDay, 1);

        }
      }
    } else {
      generateDistrictMetrics($dbSlave, $dbMaster, $District, $nextReportWeekEndDay, 6);
    }
  }

	echo "\n";
}

echo "\n";

$time_elapsed_secs = microtime(true) - $start;

echo 'Total Execution Time: ' . $time_elapsed_secs . ' Seconds';

echo "\n";

function generateLocationMetrics($dbSlave, $dbMaster, $Location, $nextReportWeekEndDay, $numberOfReportsToGenerate) {
  if($numberOfReportsToGenerate == 1) {

    $reportWeekEndDay = $nextReportWeekEndDay;

  } else {

    $reportWeekEndDay = getLastReportWeekEndDayFromNextReportWeekEndDay($nextReportWeekEndDay);

  }

  $reportWeekStartDay = getReportWeekStartDayFromEndDay($reportWeekEndDay);

  calculateLocationTaskMetrics($dbSlave, $dbMaster, $Location, $reportWeekStartDay, $reportWeekEndDay);

  calculateLocationTaskGroupMetrics($dbSlave, $dbMaster, $Location, $reportWeekStartDay, $reportWeekEndDay);

  //This generates additional division metric reports
  if($numberOfReportsToGenerate > 1) {
    for($week = 1; $week <= $numberOfReportsToGenerate; $week++) {

      $reportWeekStartDay = getLastReportWeekEndDayFromNextReportWeekEndDay($reportWeekStartDay);

      $reportWeekEndDay = getLastReportWeekEndDayFromNextReportWeekEndDay($reportWeekEndDay);

      calculateLocationTaskMetrics($dbSlave, $dbMaster, $Location, $reportWeekStartDay, $reportWeekEndDay);

      calculateLocationTaskGroupMetrics($dbSlave, $dbMaster, $Location, $reportWeekStartDay, $reportWeekEndDay);
    }
  }
}

function generateDistrictMetrics($dbSlave, $dbMaster, $District, $nextReportWeekEndDay, $numberOfReportsToGenerate) {


  if($numberOfReportsToGenerate == 1) {

    $reportWeekEndDay = $nextReportWeekEndDay;

  } else {

    $reportWeekEndDay = getLastReportWeekEndDayFromNextReportWeekEndDay($nextReportWeekEndDay);

  }

  $reportWeekStartDay = getReportWeekStartDayFromEndDay($reportWeekEndDay);

  //Calculate for the last week.
  calculateMetricsForDistrictBetweenTwoDays($dbSlave, $dbMaster, $District, $reportWeekStartDay, $reportWeekEndDay);

  //This generates additional division metric reports
  if($numberOfReportsToGenerate > 1) {
    for($week = 1; $week <= $numberOfReportsToGenerate; $week++) {
      $reportWeekStartDay = getLastReportWeekEndDayFromNextReportWeekEndDay($reportWeekStartDay);
      $reportWeekEndDay = getLastReportWeekEndDayFromNextReportWeekEndDay($reportWeekEndDay);
      calculateMetricsForDistrictBetweenTwoDays($dbSlave, $dbMaster, $District, $reportWeekStartDay, $reportWeekEndDay);
    }
  }
}

function generateDivisionMetrics($dbSlave, $dbMaster, $Division, $nextReportWeekEndDay, $numberOfReportsToGenerate) {


  if($numberOfReportsToGenerate == 1) {

    $reportWeekEndDay = $nextReportWeekEndDay;

  } else {

    $reportWeekEndDay = getLastReportWeekEndDayFromNextReportWeekEndDay($nextReportWeekEndDay);

  }

  $reportWeekStartDay = getReportWeekStartDayFromEndDay($reportWeekEndDay);

  //Calculate for the last week.
  calculateMetricsForDivisionBetweenTwoDays($dbSlave, $dbMaster, $Division, $reportWeekStartDay, $reportWeekEndDay);

  //This generates additional division metric reports
  if($numberOfReportsToGenerate > 1) {
    for($week = 1; $week <= $numberOfReportsToGenerate; $week++) {
      $reportWeekStartDay = getLastReportWeekEndDayFromNextReportWeekEndDay($reportWeekStartDay);
      $reportWeekEndDay = getLastReportWeekEndDayFromNextReportWeekEndDay($reportWeekEndDay);
      calculateMetricsForDivisionBetweenTwoDays($dbSlave, $dbMaster, $Division, $reportWeekStartDay, $reportWeekEndDay);
    }
  }
}

function calculateMetricsForDistrictBetweenTwoDays($dbSlave, $dbMaster, $District, $reportWeekStartDay, $reportWeekEndDay) {

  $location_IDs = getLocationIdsFromDistrict($dbSlave, $District->id);

  $totalLocations = count($location_IDs);

  if($totalLocations > 0) {

    $Results = get_summary_from_multiple_locations_between_two_dates($dbSlave, $location_IDs, $reportWeekStartDay, $reportWeekEndDay);

    if($Results) {

      //Maybe calculate completed percentage here?

      $unresolved_violations = 0;

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

      $q3 = $dbMaster->prepare("INSERT INTO weekly_district_task_metrics (organization,district,week_end_day,completed_tasks,missed_tasks,violations, unresolved_violations, locations, location_with_completed_task,completion_percentage) VALUES (?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE completed_tasks = ?, missed_tasks = ?, violations = ?, unresolved_violations = ?, locations = ?, location_with_completed_task = ?, completion_percentage = ?");

      $q3->execute(array($District->organization, $District->id, $reportWeekEndDay, $Results->completed_scheduled, $Results->missed, $Results->violations, $unresolved_violations, $totalLocations, $locationsWithCompletedTask, $completionPercentage, $Results->completed_scheduled, $Results->missed, $Results->violations, $unresolved_violations, $totalLocations, $locationsWithCompletedTask, $completionPercentage));
    }
  }
}

function calculateMetricsForDivisionBetweenTwoDays($dbSlave, $dbMaster, $Division, $reportWeekStartDay, $reportWeekEndDay) {

  $location_IDs = getLocationIdsFromDivision($dbSlave, $Division->id);

  $totalLocations = count($location_IDs);

  if($totalLocations > 0) {

    $Results = get_summary_from_multiple_locations_between_two_dates($dbSlave, $location_IDs, $reportWeekStartDay, $reportWeekEndDay);

    if($Results) {

      //Maybe calculate completed percentage here?

      $unresolved_violations = 0;

      $locationsWithCompletedTask = 0;

      $completionPercentage = 0;

      $completedScheduledTasks = 0;

      if($Results->completed_scheduled) {
        $completedScheduledTasks = $Results->completed_scheduled;
      }

      $totalCompletedPlusMissed = $completedScheduledTasks + $Results->missed;

      if($totalCompletedPlusMissed > 0) {
        $completionPercentage = $completedScheduledTasks / $totalCompletedPlusMissed * 100;

        //We should be smarter here...  Or triggering an event to handle.
        if($completionPercentage > 100) {
          $completionPercentage = 100;
        }

      }

      $q3 = $dbMaster->prepare("INSERT INTO weekly_division_task_metrics (organization,division,week_end_day,completed_tasks,missed_tasks,violations, unresolved_violations, locations, location_with_completed_task,completion_percentage) VALUES (?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE completed_tasks = ?, missed_tasks = ?, violations = ?, unresolved_violations = ?, locations = ?, location_with_completed_task = ?, completion_percentage = ?");

      $q3->execute(array($Division->organization, $Division->id, $reportWeekEndDay, $completedScheduledTasks, $Results->missed, $Results->violations, $unresolved_violations, $totalLocations, $locationsWithCompletedTask, $completionPercentage, $completedScheduledTasks, $Results->missed, $Results->violations, $unresolved_violations, $totalLocations, $locationsWithCompletedTask, $completionPercentage));
    }
  }
}

function getNumberOfDaysBetweenTwoDays($day1, $day2) {
  return date_diff(
        date_create($day2),  
        date_create($day1)
    )->format('%a');
}

function get_summary_from_multiple_locations_between_two_dates($db, $location_IDs, $reportWeekStartDay, $reportWeekEndDay) {

  $location_array_implosions = implode(',', $location_IDs);

  //Run the magical query
  $r = $db->prepare("SELECT count(R.id) total, sum(case when R.completed = 'Y' AND R.task_block IS NOT NULL then 1 else 0 end) completed_scheduled, sum(case when R.completed = 'N' then 1 else 0 end) missed, sum(case when R.completed = 'NA' then 1 else 0 end) notavailable, sum(case when R.corrective_action IS NOT NULL AND completed = 'Y' then 1 else 0 end) violations, sum(case when R.completed = 'Y' AND R.task_block IS NULL then 1 else 0 end) completed_not_scheduled from task_result R WHERE R.location IN (" . $location_array_implosions . ") AND R.day BETWEEN ? AND ? ORDER BY violations DESC;");

  $r->execute(array($reportWeekStartDay, $reportWeekEndDay));

  return $r->fetch(PDO::FETCH_OBJ);
}

function getLocationIdsFromDistrict($db, $DistrictId) {

  $LocationIds = array();

  $r = $db->prepare("SELECT location FROM organization_district_location WHERE organization_district = ?");

  $r->execute(array($DistrictId));

  $Result = $r->fetchAll(PDO::FETCH_OBJ);

  foreach($Result AS $Row) {
    $LocationIds[] = $Row->location;
  }

  return $LocationIds;
}

function getLocationIdsFromDivision($db, $DivisionId) {

  $LocationIds = array();

  $r = $db->prepare("SELECT ODL.location FROM organization_district_location ODL LEFT JOIN organization_district ON organization_district.id = ODL.organization_district WHERE organization_district.organization_division = ?");

  $r->execute(array($DivisionId));

  $Result = $r->fetchAll(PDO::FETCH_OBJ);

  foreach($Result AS $Row) {
    $LocationIds[] = $Row->location;
  }

  return $LocationIds;
}

function getDistrictsFromOrganization($db, $OrganizationId) {

	//We need to determine what tasks should be completed and what tasks have not been completed.
	$r = $db->prepare("SELECT organization_district.organization AS organization, organization_district.id, organization_district.name, groupeddtm.MaxWeekEndDay FROM organization_district LEFT JOIN (SELECT district,MAX(week_end_day) AS MaxWeekEndDay FROM weekly_district_task_metrics GROUP BY district) groupeddtm ON organization_district.id = groupeddtm.district WHERE organization_district.organization = ?");

	$r->execute(array($OrganizationId));

	return $r->fetchAll(PDO::FETCH_OBJ);

}


function getDivisionsFromOrganization($db, $OrganizationId) {

  //We need to determine what tasks should be completed and what tasks have not been completed.
  $r = $db->prepare("SELECT organization_division.organization AS organization, organization_division.id, organization_division.name, groupeddtm.MaxWeekEndDay FROM organization_division LEFT JOIN (SELECT division,MAX(week_end_day) AS MaxWeekEndDay FROM weekly_division_task_metrics GROUP BY division) groupeddtm ON organization_division.id = groupeddtm.division WHERE organization_division.organization = ?");

  $r->execute(array($OrganizationId));

  return $r->fetchAll(PDO::FETCH_OBJ);

}


function getLocationsFromOrganization($db, $OrganizationId) {

  //We need to determine what tasks should be completed and what tasks have not been completed.
  $r = $db->prepare("SELECT organization_location.organization, location.name, location.id AS id,location.timezone, groupedltm.MaxWeekEndDay FROM organization_location LEFT JOIN location ON location.id = organization_location.location LEFT JOIN (SELECT location,MAX(week_end_day) AS MaxWeekEndDay FROM weekly_location_task_metrics GROUP BY location) groupedltm ON organization_location.location = groupedltm.location WHERE organization_location.organization = ?");

  $r->execute(array($OrganizationId));

  return $r->fetchAll(PDO::FETCH_OBJ);

}

function getNextReportWeekEndDay($timezone, $dayWeekStarts) {

  $timeAtLocation = new DateTime("now", new DateTimeZone($timezone));

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

  $mostRecentReportEnded->add(new DateInterval('P7D'));

  return $mostRecentReportEnded->format("Y-m-d");

}

function getCurrentReportEndDay($timezone) {

  $currentReportEndDayDateTime = new DateTime("now", new DateTimeZone($timezone));

  //To ensure we have a full days worth of data.
  $currentReportEndDayDateTime->sub(new DateInterval('P1D'));

  return $currentReportEndDayDateTime->format('Y-m-d');

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

function calculateLocationTaskGroupMetrics($dbSlave, $dbMaster, $Location, $reportWeekEndDay) {

  $reportWeekStartDay = getReportWeekStartDayFromEndDay($reportWeekEndDay);

  $TaskBlocks = get_task_blocks_from_location_with_completed_task_for_this_week($dbSlave, $Location->id, $reportWeekStartDay, $reportWeekEndDay);

  if($TaskBlocks && count($TaskBlocks) > 0) {

    foreach($TaskBlocks AS $TaskBlock) {

      $data_is_valid = false;

      $q1 = $dbSlave->prepare("SELECT MIN(marked_at) AS start_time, MAX(marked_at) AS stop_time, count(DISTINCT(task)) AS total FROM task_result WHERE completed = 'Y' AND task_block = ? AND day BETWEEN ? AND ? GROUP BY task_block,day");

      $q1->execute(array($TaskBlock->task_block, $reportWeekStartDay, $reportWeekEndDay));

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

        $q3->execute(array($Location->id, $TaskBlock->task_block, $reportWeekEndDay, $averageStartTime, $startTimeVolatility, $averageStopTime, $stopTimeVolatility, $averageStartTime, $startTimeVolatility, $averageStopTime, $stopTimeVolatility));
      }
    }
  }

}

function calculateLocationTaskMetrics($dbSlave, $dbMaster, $Location, $reportWeekStartDay, $reportWeekEndDay) {

    //Check to see if we have an entry with this reportEndDay
    $Results = get_summary_from_single_location_between_two_dates($dbSlave, $Location->id, $reportWeekStartDay, $reportWeekEndDay);

    $ViolationObj = get_unresolved_violations_from_location_between_two_dates($dbSlave, $Location->id, $reportWeekStartDay, $reportWeekEndDay);

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

    $r->execute(array($Location->organization,$Location->id, $reportWeekEndDay, $Results->completed_scheduled, $Results->missed, $Results->violations, $ViolationObj->resolvedViolations, $ViolationObj->followUpsRequired, $completionPercentage, $Results->completed_scheduled, $Results->missed, $Results->violations, $ViolationObj->resolvedViolations, $ViolationObj->followUpsRequired, $completionPercentage));
}

function getLastReportWeekEndDayFromNextReportWeekEndDay($nextReportWeekEndDay) {
  $reportWeekEndDay = new DateTime($nextReportWeekEndDay);

  $reportWeekEndDay->sub(new DateInterval('P7D'));

  return $reportWeekEndDay->format('Y-m-d');
}

function getReportWeekStartDayFromEndDay($reportWeekEndDay) {
  $reportWeekStartDay = new DateTime($reportWeekEndDay);

  $reportWeekStartDay->sub(new DateInterval('P6D'));

  return $reportWeekStartDay->format('Y-m-d');
}

function get_task_blocks_from_location_with_completed_task_for_this_week($db, $locationId, $reportWeekStartDay, $reportWeekEndDay) {

  $r = $db->prepare("SELECT DISTINCT(task_block) AS task_block FROM task_result WHERE day BETWEEN ? AND ? AND location = ? AND completed = 'Y'");

  $r->execute(array($reportWeekStartDay, $reportWeekEndDay, $locationId));

  return $r->fetchAll(PDO::FETCH_OBJ);
}

function get_summary_from_single_location_between_two_dates($db, $locationId, $reportWeekStartDay, $reportWeekEndDay) {
  //Run the magical query
  $r = $db->prepare("SELECT count(R.id) total, sum(case when R.completed = 'Y' AND R.task_block IS NOT NULL then 1 else 0 end) completed_scheduled, sum(case when R.completed = 'N' then 1 else 0 end) missed, sum(case when R.completed = 'NA' then 1 else 0 end) notavailable, sum(case when R.corrective_action IS NOT NULL AND completed = 'Y' then 1 else 0 end) violations, sum(case when R.completed = 'Y' AND R.task_block IS NULL then 1 else 0 end) completed_not_scheduled from task_result R WHERE R.location = ? AND R.day BETWEEN ? AND ? ORDER BY violations DESC;");

  $r->execute(array($locationId, $reportWeekStartDay, $reportWeekEndDay));

	return $r->fetch(PDO::FETCH_OBJ);
}

function get_unresolved_violations_from_location_between_two_dates($db, $locationId, $reportWeekStartDay, $reportWeekEndDay) {

  $ViolationObj = new stdClass();

  $resolvedViolations = 0;

  $followUpsRequired = 0;

  $CorrectiveActions = getLocationCorrectiveActions($db, $locationId);

  $r = $db->prepare("SELECT task_result.id AS taskResultID,task_result.result, task.id AS taskID, task_result.day, task.title, task_result.corrective_action AS description, task_result.marked_at, task_result.task_block AS taskBlockID FROM task_result LEFT JOIN task ON task.id = task_result.task WHERE task_result.location = ? AND task_result.completed = 'Y' AND task_result.corrective_action IS NOT NULL AND task_result.day BETWEEN ? AND ? ORDER BY task_result.id DESC");

  $r->execute(array($locationId, $reportWeekStartDay, $reportWeekEndDay));

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
