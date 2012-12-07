<?php
/**
 * Created by JetBrains PhpStorm.
 * User: Miro
 * Date: 27.11.12
 * Time: 11:44
 * To change this template use File | Settings | File Templates.
 */

namespace Syrup\ExtractorBundle\Extractor;

use \Syrup\ExtractorBundle\Extractor\Extractor;
use Keboola\StorageApi\Table;

class DummyExtractor extends Extractor
{
	protected $_extractorName = 'dummy';

	protected function _process($config)
	{
		// Get some data
		$data = array(
			array('id', 'col1', 'col2', 'col3'),
			array('1', 'a', 'b', 'c'),
			array('2', 'd', 'e', 'f'),
			array('3', 'g', 'h', 'i'),
			array('4', 'j', 'k', 'l'),
		);

		$table = new Table($this->_storageApi, 'in.c-main.test');

		$table->setFromArray($data, $hasHeader = true);

		return $table;
	}
}
