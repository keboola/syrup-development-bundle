<?php
/**
 * RegisterBundleCommand.php
 *
 * @author: Miroslav Čillík <miro@keboola.com>
 * @created: 18.1.13
 */

namespace Syrup\CoreBundle\RegisterBundleCommand;

use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class RegisterBundleCommand extends ContainerAwareCommand
{
	protected function configure()
	{
		$this
			->setName('syrup:register-bundle')
			->setDescription('Register your bundle to Syrup. It will be added to composer.json and app/AppKernel.php')
			->addArgument('name', InputArgument::REQUIRED, 'Name of your bundle i.e. Syrup/CoreBundle')
			->addArgument('url', InputArgument::REQUIRED, 'Repository url where your bundle resides')
		;
	}

	protected function execute(InputInterface $input, OutputInterface $output)
	{
		$name = $input->getArgument('name');
		$url = $input->getArgument('url');

		//$output->writeln($text);
	}
}
