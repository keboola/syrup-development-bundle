<?php

namespace Syrup\ExtractorBundle\Controller;

use Symfony\Component\DependencyInjection\ContainerAware;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use Keboola\StorageApi\Client;
use Syrup\ExtractorBundle\Extractor\ExtractorInterface;

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
    public function runAction($extractorName)
    {
	    /**
	     * @var ExtractorInterface $extractor
	     */
	    $extractor = $this->container->get('syrup.extractor_factory')->get($this->_storageApi, $extractorName);
	    $this->container->get('logger');
	    //$extractor->setContainer($this->container);
	    $extractor->run();

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
