# Script used to launch python backbone components
import importlib
import json
import secrets
import string
import sys
from types import ModuleType

from py4j.clientserver import ClientServer, JavaParameters, PythonParameters

if __name__ == '__main__':
    args = sys.argv[1:]
    entrypoint_import: str = args[0]
    first_init: bool = args[1] == 'component'

    # Import the backbone module to be used
    module: ModuleType = importlib.import_module(entrypoint_import)

    # Generate an authentication token for this session
    auth_token = ''.join(secrets.choice(string.ascii_uppercase + string.digits)
                         for i in range(16))

    # Get appropriate entry point
    if first_init:
        entry_point = module.get_component_def()
    else:
        entry_point = module.get_do_fn()

    # Bootup python endpoint
    gateway = ClientServer(
        java_parameters=JavaParameters(port=0, auth_token=auth_token),
        python_parameters=PythonParameters(port=0, auth_token=auth_token),
        python_server_entry_point=entry_point
    )

    java_port: int = gateway.java_parameters.port
    python_port: int = gateway.python_parameters.port

    # Write vars out to JSON
    with open('python_bridge_meta.json', 'rw') as f:
        json.dump({
            'token': auth_token,
            'java_port': java_port,
            'python_port': python_port
        }, f)

    # Create monitor file used by java process to indicate gateway init complete
    with open('python_bridge_meta.done', 'rw') as f:
        f.writelines('done')
