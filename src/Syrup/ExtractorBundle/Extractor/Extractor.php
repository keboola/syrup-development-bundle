<?php

/**
 * @author Miroslav Cillik <miro@keboola.com>
 * Date: 23.11.12
 * Time: 17:04
 */

namespace Syrup\ExtractorBundle\Extractor;

use Syrup\ExtractorBundle\Extractor\ExtractorInterface;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Config\Reader;
use Keboola\StorageApi\Table;
use Symfony\Component\DependencyInjection\ContainerAware;

class Extractor extends ContainerAware implements ExtractorInterface
{
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected $_storageApi;

	/**
	 * @var \Monolog\Logger
	 */
	protected $_log;

	protected $_sysBucketName = 'extractorName';

	public function __construct(Client $storageApi, $log)
	{
		$this->_storageApi = $storageApi;
		$this->_log = $log;
		Reader::$client = $this->_storageApi;
	}

	public function run()
	{
		$config = $this->getConfig();

		$result = $this->_process($config);

		if ($result) {
			if (is_array($result)) {
				foreach ($result as $table) {
					$this->_saveTable($table);
				}
			} else {
				$this->_saveTable($result);
			}
		}
	}

	/**
	 * Override this - get data from remote services
	 */
	protected function _process($config)
	{
		return false;
	}

	protected function _saveTable($table)
	{
		if ($table instanceof Table) {
			$table->save();
		} else {
			throw new \Exception("Result must be instance of Keboola\\StorageApi\\Table or array of these instances.");
		}
	}

	/**
	 * Reads configuration from StorageApi
	 *
	 * could be empty - extractor with no configuration
	 */
	public function getConfig()
	{
		if ($this->_storageApi->bucketExists('sys.' . $this->_sysBucketName)) {
			return Reader::read('sys.' . $this->_sysBucketName);
		} else {
			//throw new \Exception("SYS bucket doesn't exists");
			return array();
		}
	}
}
