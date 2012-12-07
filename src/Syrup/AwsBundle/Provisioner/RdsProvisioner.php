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
	/**
	 * @var AmazonRDS
	 */
	protected  $_rds;

	protected function _process($config)
	{
		echo "Provisioning stuff";

		$this->_rds = new AmazonRDS(array(
			'key'       => 'key',
			'secret'    => 'secret',
			'token'     => 'token'
		));
	}

}
