<?php
/**
 * Created by JetBrains PhpStorm.
 * User: Miro
 * Date: 26.11.12
 * Time: 11:02
 * To change this template use File | Settings | File Templates.
 */
namespace Syrup\ExtractorBundle\Extractor;

use Keboola\StorageApi\Client;
use Monolog\Logger;

class ExtractorFactory
{
	protected $_logger;

	protected $_extractorsConfig;

	public function __construct(Logger $logger, $extractorsConfig)
	{
		$this->_logger = $logger;
		$this->_extractorsConfig = $extractorsConfig;
	}

	/**
	 * @param \Keboola\StorageApi\Client $storageApi
	 * @param $extractorName
	 * @return ExtractorInterface $extractor
	 * @throws \Exception
	 */
	public function get(Client $storageApi, $extractorName)
	{
		if (isset($this->_extractorsConfig[strtolower($extractorName)])) {

			$extractorConfig = $this->_extractorsConfig[strtolower($extractorName)];

			if (isset($extractorConfig['class'])) {
				$className = $extractorConfig['class'];
				if (class_exists($extractorConfig['class'])) {
					return new $className($storageApi, $this->_logger);
				} else {
					$error = 'Extractor class "'.$extractorConfig['class'].'" does not exists';
				}
			} else {
				$error = 'Missing class definition in configuration.';
			}
		} else {
			$error = 'Missing configuration or wrong extractor name';
		}

		throw new \Exception('Failed to load extractor. ' . $error);
	}

}
