<?php

namespace Syrup\SlevomatBundle\Controller;

use Symfony\Bundle\FrameworkBundle\Controller\Controller;

class DefaultController extends Controller
{
    public function indexAction($name)
    {
        return $this->render('SyrupSlevomatBundle:Default:index.html.twig', array('name' => $name));
    }
}
