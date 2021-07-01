from execution.robot_basis import RobotManager as BasisRobotMananger
import click


@click.command()
@click.argument("robot_id")
def robot(robot_id):
    robot_manager = BasisRobotMananger()
    robot_manager.run_robot_by_id(robot_id)


if __name__ == '__main__':
    robot()
