from monday.framework.stateful.metadata import MetaData
from monday.framework.stateful.obj import StatefulObject


def main():
    metadata = MetaData(name='monday')
    obj = StatefulObject(metadata=metadata)
    obj.initialize()
    obj.tick()
    obj.tick()
    obj.warm_up()
    obj.tick()
    obj.tick()
    obj.boost()
    obj.tick()
    obj.tick()
    obj.slow_down()
    obj.tick()
    obj.tick()


if __name__ == '__main__':
    main()
