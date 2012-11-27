<?php
/**
 * @author Miroslav Cillik <miro@keboola.com>
 * Date: 26.11.12
 * Time: 14:40
 */

namespace Syrup\ExtractorBundle\Monolog\Formatter;

use Monolog\Formatter\JsonFormatter;

class SyrupJsonFormatter extends JsonFormatter
{
	protected $_appName;
	protected $_runId;
	protected $_extractorName = '';
	protected $_logData;

	public function __construct($appName)
	{
		$this->_appName = $appName;
		$this->_runId = $appName . '-';
		if ($this->_extractorName) {
			$this->_runId .= $this->_extractorName . '-';
		}
		$this->_runId .= md5(microtime());
	}

	public function setExtractorName($extractorName)
	{
		$this->_extractorName = $extractorName;
	}

	public function setLogData($logData)
	{
		$this->_logData = $logData;
	}

	/**
	 * {@inheritdoc}
	 */
	public function format(array $record)
	{
		$record['app']          = $this->_appName;
		$record['extractor']    = $this->_extractorName;
		$record['priority']     = $record['level_name'];
		$record['user']         = $this->_logData;

		unset($record['level_name']);
		unset($record['level']);
		unset($record['channel']);

		return json_encode($record);
	}

	/**
	 * {@inheritdoc}
	 */
	public function formatBatch(array $records)
	{
		return json_encode($records);
	}
}
