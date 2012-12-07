<?php

/**
 * @author Miroslav Cillik <miro@keboola.com>
 * Date: 23.11.12
 * Time: 17:04
 */

namespace Syrup\ComponentBundle\Component;

use Syrup\ComponentBundle\Component\ComponentInterface;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Config\Reader;
use Keboola\StorageApi\Table;
use Symfony\Component\DependencyInjection\ContainerAware;

class Component extends ContainerAware implements ComponentInterface
{
	/**
	 * @var \Keboola\StorageApi\Client
	 */
	protected $_storageApi;

	/**
	 * @var \Monolog\Logger
	 */
	protected $_log;

	protected $_name = 'componentName';

	protected $_prefix = '';

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
		if ($this->_storageApi->bucketExists('sys.c-' . $this->_prefix . '-' . $this->_name)) {
			return Reader::read('sys.c-' . $this->_prefix . '-' . $this->_name);
		} else {
			return array();
		}
	}
}
