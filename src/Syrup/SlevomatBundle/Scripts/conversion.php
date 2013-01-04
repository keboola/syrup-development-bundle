<?php

function toCsvEscaped($line) {
	$line = str_replace('"', '""', $line);
	$line = rtrim($line,"\n");
	return '"' . str_replace("\t", '","', $line) . '"' . "\n";
};

function toCsv($line) {
	return stripcslashes(toCsvEscaped($line));
};

function toJson($line, $rowNumber) {
	static $header = array();
	$csvLine = toCsv($line);
	$parsed = str_getcsv($csvLine, ',', '"', '"');
	if ($rowNumber == 1) {
		$header = $parsed;
		return;
	}

	return json_encode(array_combine($header, $parsed)) . "\n";
};

$fh = fopen('php://stdin', 'r');
$stderr = fopen('php://stderr', 'w');
if (!$fh) {
	fwrite($stderr, "Error on reading stdin");
	die(1);
}

$availableConversions = array(
	'csv',
	'csvEscaped',
	'json',
);

$conversion = reset($availableConversions);
if (isset($argv[1])) {
	if (!in_array($argv[1], $availableConversions)) {
		fwrite($stderr, "Invalid conversion type: " . $argv[1]);
		die(1);
	}
	$conversion = $argv[1];
}

$conversion = 'to' . ucfirst($conversion);

$rowNumber = 1;
while ($line = fgets($fh)) {
	print $conversion($line, $rowNumber);
	$rowNumber++;
}