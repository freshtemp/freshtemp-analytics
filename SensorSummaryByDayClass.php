<?php
class SensorSummaryByDayClass
{

  public function __construct($Location, $Sensor, $Equipment, $Day, $Timezone, $lowerLimit, $upperLimit, $txInterval) {

    $this->Location = $Location;

    $this->Sensor = $Sensor;

    $this->Equipment = $Equipment;

    $this->Day = $Day;

    $this->Timezone = $Timezone;

    $this->dayStartTimeUTC = $this->getUTCDateTimeFromDayAndHourMinuteSeconds('00:00:00');

    $this->dayEndTimeUTC = $this->getUTCDateTimeFromDayAndHourMinuteSeconds('23:59:59');

    $this->lowerLimit = (float)$lowerLimit;

    $this->upperLimit = (float)$upperLimit;

    $this->txInterval = $txInterval;

    $this->totalMeasurements = 0;

    $this->avgTemp = null;

    $this->longestGapInDataInSeconds = 0; //Default to entire day

    $this->percentMeasurementsGreaterThanUpperLimit = null;

    $this->percentMeasurementsLessThanLowerLimit = null;

    $this->numberOfAlarmsEnabled = 0;

    $this->numberOfAlarmsTriggered = 0;

    $this->totalMeasurementsAboveUpperLimit = 0; //Not saved to DB

    $this->totalMeasurementsBelowLowerLimit = 0; //Not saved to DB

    $this->temps = []; //Not saved to DB

    $this->totalNumberOfSecondsBetweenMeasurements = 0;

  }

  public function getNumberOfAlarmsTriggeredAndEnabled($db) {
    //$sth = $db->prepare("SELECT count(id) AS total FROM events WHERE equipment = :equipment AND started BETWEEN :day_start_time AND :day_end_time");

    //$sth->bindParam(':equipment', $this->Equipment, PDO::PARAM_INT);
    //$sth->bindParam(':day_start_time', $this->dayStartTimeUTC, PDO::PARAM_STR);
    //$sth->bindParam(':day_end_time', $this->dayEndTimeUTC, PDO::PARAM_STR);

    //$sth->execute();

    //$this->numberOfAlarmsTriggered = $sth->rowCount();

    $sth = $db->prepare("SELECT count(*) FROM equipment_group_equipment E LEFT JOIN alert_temp A ON A.equipment_group = E.equipment_group WHERE E.equipment = :equipment AND A.deleted IS NULL");

    $sth->bindParam(':equipment', $this->Equipment, PDO::PARAM_INT);

    $sth->execute();

    $this->numberOfAlarmsEnabled = $sth->rowCount();

  }

  public function getUTCDateTimeFromDayAndHourMinuteSeconds($HoursMinutesSeconds) {
    $utcDateTime = DateTime::createFromFormat('Y-m-d H:i:s', $this->Day . ' ' . $HoursMinutesSeconds,new DateTimeZone($this->Timezone));
    $utcDateTime->setTimezone(new DateTimeZone('UTC'));
    return $utcDateTime->format("Y-m-d " . $HoursMinutesSeconds);
  }

  public function fetchAndAnalysisSensorData($db) {

    $sth = $db->prepare("SELECT id,created,tempval FROM sample WHERE target = :equipment AND created BETWEEN :day_start_time AND :day_end_time ORDER BY id DESC");

    $sth->bindParam(':equipment', $this->Equipment, PDO::PARAM_INT);
    $sth->bindParam(':day_start_time', $this->dayStartTimeUTC, PDO::PARAM_STR);
    $sth->bindParam(':day_end_time', $this->dayEndTimeUTC, PDO::PARAM_STR);

    $sth->execute();

    $totalTimeOffline = 0;

    while ($row = $sth->fetchObject()) {

      $temperature = $row->tempval;

      if($this->totalMeasurements > 0) {

        $numberOfSecondsBetweenMeasurements = $lastMeasurementTime - strtotime($row->created);

        $this->totalNumberOfSecondsBetweenMeasurements += $numberOfSecondsBetweenMeasurements;

        if($numberOfSecondsBetweenMeasurements > $this->longestGapInDataInSeconds) {
          $this->longestGapInDataInSeconds = $numberOfSecondsBetweenMeasurements;
        }

        if($temperature > $maxTemperature) {
          $maxTemperature = $temperature;
        }

        if($temperature > $this->upperLimit) {
          $this->totalMeasurementsAboveUpperLimit++;
        }

        if($temperature < $this->lowerLimit) { 
          $this->totalMeasurementsBelowLowerLimit++;
        }
      } else {
        $maxTemperature = $temperature;
      }

      $this->temps[] = $temperature;

      $lastMeasurementTime = strtotime($row->created);

      $this->totalMeasurements++;
    }
  }

  public function computeSensorMetrics() {
    if($this->totalMeasurements > 0) {
      $this->percentMeasurementsGreaterThanUpperLimit = number_format($this->totalMeasurementsAboveUpperLimit / $this->totalMeasurements, 2) * 100;
      $this->percentMeasurementsLessThanLowerLimit = number_format($this->totalMeasurementsBelowLowerLimit / $this->totalMeasurements, 2) * 100;

      $this->avgTemp = number_format(array_sum($this->temps) / count($this->temps),1);
    }
  }

  public function insertIntoDatabase($db) {
    try {

      $InsertStatement = $db->prepare("INSERT INTO sensor_summary_by_day (location,sensor,equipment,day,lower_limit,upper_limit,tx_interval, total_measurements,avg_temp,longest_gap_in_data_in_seconds,percent_measurements_greater_than_upper_limit,percent_measurements_less_than_lower_limit,number_of_alarms_enabled,number_of_alarms_triggered, total_seconds_between_measurements) VALUES (:location,:sensor,:equipment,:day,:lower_limit,:upper_limit,:tx_interval,:total_measurements,:avg_temp,:longest_gap_in_data,:measurements_greater_than_upper_limit,:measurements_less_than_lower_limit,:number_of_alarms_enabled,:number_of_alarms_triggered, :total_seconds_between_measurements) ON DUPLICATE KEY UPDATE total_measurements = :total_measurements_update, avg_temp = :avg_temp_update, longest_gap_in_data_in_seconds = :longest_gap_in_data_update, percent_measurements_greater_than_upper_limit = :measurements_greater_than_upper_limit_update, percent_measurements_less_than_lower_limit = :measurements_less_than_lower_limit_update, total_seconds_between_measurements = :total_seconds_between_measurements_update");

      return $InsertStatement->execute($this->returnInsertArray());

    } catch (PDOException $e) {

      return $e->getMessage();
    }
  }

  // method declaration
  public function returnInsertArray() {
    return array(
      ":location" => $this->Location,
      ":sensor" => $this->Sensor,
      ":equipment" => $this->Equipment,
      ":day" => $this->Day,
      ":lower_limit" => $this->lowerLimit,
      ":upper_limit" => $this->upperLimit,
      ":tx_interval" => $this->txInterval,
      ":total_measurements" => $this->totalMeasurements,
      ":avg_temp" => $this->avgTemp,
      ":longest_gap_in_data" => $this->longestGapInDataInSeconds,
      ":measurements_greater_than_upper_limit" => $this->percentMeasurementsGreaterThanUpperLimit,
      ":measurements_less_than_lower_limit" => $this->percentMeasurementsLessThanLowerLimit,
      ":number_of_alarms_enabled" => $this->numberOfAlarmsEnabled,
      ":number_of_alarms_triggered" => $this->numberOfAlarmsTriggered,
      ":total_seconds_between_measurements" => $this->totalNumberOfSecondsBetweenMeasurements,
      ":total_measurements_update" => $this->totalMeasurements,
      ":avg_temp_update" => $this->avgTemp,
      ":longest_gap_in_data_update" => $this->longestGapInDataInSeconds,
      ":measurements_greater_than_upper_limit_update" => $this->percentMeasurementsGreaterThanUpperLimit,
      ":measurements_less_than_lower_limit_update" => $this->percentMeasurementsLessThanLowerLimit,
      ":total_seconds_between_measurements_update" => $this->totalNumberOfSecondsBetweenMeasurements
    );
  }
}