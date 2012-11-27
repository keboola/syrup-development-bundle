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

	public function __construct(Logger $logger)
	{
		$this->_logger = $logger;
	}

	public function get(Client $storageApi, $extractorName)
	{
		$className = '\\Syrup\\ExtractorBundle\\Extractor\\' . ucfirst($extractorName) . 'Extractor';
		if (class_exists($className)) {
			/**
			 * @var ExtractorInterface $extractor
			 */
			return new $className($storageApi, $this->_logger);
		} else {
			$this->_logger->err("Extractor does not exist");
			throw new \Exception('Extractor does not exist');
		}
	}

}
