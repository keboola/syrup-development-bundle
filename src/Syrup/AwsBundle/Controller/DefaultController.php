<?php

namespace Syrup\AwsBundle\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;

class DefaultController extends Controller
{
    public function indexAction($name)
    {
        return $this->render('SyrupAwsBundle:Default:index.html.twig', array('name' => $name));
    }
}
