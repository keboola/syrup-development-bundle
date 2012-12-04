<?php

namespace Syrup\GoogleAnalyticsBundle\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;

class DefaultController extends Controller
{
    public function indexAction($name)
    {
        return $this->render('SyrupGoogleAnalyticsBundle:Default:index.html.twig', array('name' => $name));
    }
}
