<?php

/**
 * GoogleAnalytics Extractor
 *
 * @author: Miroslav Cillik <miro@keboola.com>
 * @created: 4.12.12
 */

namespace Syrup\GoogleAnalyticsBundle\Extractor;

use Syrup\ComponentBundle\Component\Component;

class GoogleAnalyticsExtractor extends Extractor
{
	protected $_name = 'googleAnalytics';
	protected $_prefix = 'ex';

	protected function _process($config)
	{
		// Init Google API

		foreach($config as $account)
		{
			//do stuff
		}
	}
}
