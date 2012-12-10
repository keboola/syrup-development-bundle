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
	    $request = $this->getRequest();

	    /**
	     * @var ComponentInterface $component
	     */
	    $component = $this->container->get('syrup.component_factory')->get($this->_storageApi, $componentName);
	    $this->container->get('logger');
	    $component->run($request->getContent());

	    $response = new Response(json_encode(array(
		    'status'    => 'ok'
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
