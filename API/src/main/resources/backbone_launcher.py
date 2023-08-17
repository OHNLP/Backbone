import sys

from ohnlp.toolkit.backbone.backbone_module_launcher import launch_bridge

if __name__ == "__main__":
    args = sys.argv[1:]
    launch_bridge(args[0], args[1], args[2], args[3])
