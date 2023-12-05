## Virtual env
Run `python3 -m venv .venv` to create virtual environment.

Run `source .venv/bin/activate` to activate it.

Run `pip install -r requirements.txt` to install dependencies.

Run `deactivate` to leave venv.

## Building package
To build python package archive run `python -m build`. Check `./dist` folder.

## Publishing package
Navigate to the `hyperline-developers/spark-sdk/` directory, and run:
```
./scripts/install_sdk.sh
```
