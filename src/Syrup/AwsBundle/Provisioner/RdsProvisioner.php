<?php
/**
 * AWS RDS Provisioner
 *
 * @author: Miroslav Cillik <miro@keboola.com>
 * @created: 7.12.2012
 */

namespace Syrup\AwsBundle\Provisioner;

use AmazonRDS,
	Syrup\ComponentBundle\Component\Component;

class RdsProvisioner extends Component
{
	protected $_name = 'rds-provisioner';

	/**
	 * @var AmazonRDS
	 */
	protected  $_rds;

	protected function _process($config, $params)
	{
		echo "Provisioning stuff";
		print_r($config);
		print_r($params);

		$this->_rds = new AmazonRDS(array(
			'key'       => 'key',
			'secret'    => 'secret',
			'token'     => 'token'
		));
	}

	protected function _create()
	{

	}

	protected function _delete()
	{

	}

}
