<?php

include ('SensorSummaryByDayClass.php');

$start = microtime(true);

date_default_timezone_set('UTC');

// define BASEPATH so we can load the necessary config entries, then load it
define('BASEPATH', pathinfo(__FILE__, PATHINFO_DIRNAME));

$dbMaster = new PDO('mysql:host=172.21.2.20;dbname=tempmon;charset=utf8', 'root', 'D1g1Fr3shT3mp');
$dbMaster->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);


//Only check for tasks marked null.
$sth = $dbMaster->prepare("SELECT S.id AS sensorID,L.id AS locationID, S.target AS equipmentID,L.timezone,T.lower_limit, T.upper_limit,S.tx_interval,L.name AS locationName FROM sensor S LEFT JOIN location L ON L.id = S.location LEFT JOIN target T ON T.id = S.target WHERE T.deleted IS NULL AND S.deleted IS NULL AND L.deleted IS NULL AND S.target IS NOT NULL AND S.location > 0");

$sth->execute();

$numberOfDaysToCalculateMetrics = 10;

while ($row = $sth->fetchObject()) {

  while($numberOfDaysToCalculateMetrics > 1) {

    $Day = getYesterdayAtLocationByTimeZone($row->timezone, 'Y-m-d', $numberOfDaysToCalculateMetrics);

    $SensorSummaryByDay = new SensorSummaryByDayClass($row->locationID, $row->sensorID, $row->equipmentID, $Day, $row->timezone, $row->lower_limit, $row->upper_limit, $row->tx_interval);

    $SensorSummaryByDay->getNumberOfAlarmsTriggeredAndEnabled($dbMaster);

    $SensorSummaryByDay->fetchAndAnalysisSensorData($dbMaster);

    $SensorSummaryByDay->computeSensorMetrics();
   
    $SensorSummaryByDay->insertIntoDatabase($dbMaster);

    $time_elapsed_secs = microtime(true) - $start;

    $numberOfDaysToCalculateMetrics--;
  }
}

echo "\nLocation: " . $row->locationName . "\nTotal Execution Time: " . $time_elapsed_secs . " Seconds";


function getYesterdayAtLocationByTimeZone($timezone, $dateFormat = 'Y-m-d', $numDaysToSubtract) {

  $timeAtLocation = new DateTime("now", new DateTimeZone($timezone));

  $timeAtLocation->modify('-' . $numDaysToSubtract . ' day');

  return $timeAtLocation->format($dateFormat);
}