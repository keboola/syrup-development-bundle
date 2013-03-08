<?php
/**
 * RegisterBundleCommand.php
 *
 * @author: Miroslav Čillík <miro@keboola.com>
 * @created: 18.1.13
 */

namespace Syrup\CoreBundle\Command;

use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Sensio\Bundle\GeneratorBundle\Manipulator\KernelManipulator;

class RegisterBundleCommand extends ContainerAwareCommand
{
	protected function configure()
	{
		$this
			->setName('syrup:register-bundle')
			->setDescription('Register your bundle to Syrup. This will create basic configuration for you and register bundle to app/AppKernel.php')
			->addArgument('name', InputArgument::REQUIRED, 'Name of your bundle i.e. Syrup/CoreBundle')
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		$name = $input->getArgument('name');

		//update Kernel
		$kernelManipulator = new KernelManipulator($this->getContainer()->get('kernel'));

		$nameArr = explode('/', $name);

		$bundle = array_pop($nameArr);;

		$class = '';
		$namespace = '';
		foreach ($nameArr as $name) {
			$class .= $name;
			$namespace .= $name . '\\';
		}
		$class .= $bundle;

		try {
			$kernelManipulator->addBundle($namespace . $bundle . '\\' . $class);
		} catch (\RuntimeException $e) {
			return array(
				sprintf('Bundle <comment>%s</comment> is already defined in <comment>AppKernel::registerBundles()</comment>.', $namespace.'\\'.$bundle),
				'',
			);
		}

		$output->writeln("Bundle succesfully registered to Kernel.");
	}
}
