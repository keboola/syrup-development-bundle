<?php

use Composer\Autoload\ClassLoader;
use Doctrine\Common\Annotations\AnnotationRegistry;

/**
 * @var $loader ClassLoader
 */
$loader = require __DIR__.'/../../../autoload.php';

AnnotationRegistry::registerLoader(array($loader, 'loadClass'));

return $loader;
