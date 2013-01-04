<?php

namespace Syrup\ComponentBundle\Controller;

use Symfony\Component\DependencyInjection\ContainerAware;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Keboola\StorageApi\Client;
use Syrup\ComponentBundle\Component\ComponentInterface;

class ApiController extends ContainerAware
{
	/**
	 * @var Client
	 */
	protected $_storageApi;

	public function preExecute()
	{
		$request = $this->getRequest();

		if ($request->headers->has('X-StorageApi-Token')) {
			$this->_storageApi = new Client($request->headers->get('X-StorageApi-Token'));
			$this->container->set('storageApi', $this->_storageApi);
			$this->container->get('syrup.monolog.json_formatter')->setLogData($this->_storageApi->getLogData());
		} else {
			throw new \Exception('Missing SotrageApi token');
		}
	}

	/**
	 * Abstract run action - override this in child extractors
	 * @return \Symfony\Component\HttpFoundation\Response
	 */
    public function runAction($componentName)
    {
	    set_time_limit(3600*3);

	    $request = $this->getRequest();

	    $timestart = microtime(true);
	    $component = $this->container->get('syrup.component_factory')->get($this->_storageApi, $componentName);
	    $component->run(json_decode($request->getContent(), true));
	    $duration = microtime(true) - $timestart;

	    $response = new Response(json_encode(array(
		    'status'    => 'ok',
		    'duration'  => $duration
	    )));

	    return $response;
    }

	/**
	 * @return Request
	 */
	public function getRequest()
	{
		return $this->container->get('request');
	}

}
