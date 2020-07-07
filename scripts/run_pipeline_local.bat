REM Runs a backbone pipeline locally on a computer. Note that this is not recommended beyond for debugging/testing
REM as it is not scalable and has additional local debugging.
REM GCP or Spark options are recommended for production and/or large-scale use cases

cd %~dp0

SET BACKBONE_CONFIG=NAME_OF_CONFIG_TO_USE.json

IF EXIST bin\Backbone-Core-Packaged.jar (
    java -jar bin\Backbone-Core-Packaged.jar --config=$BACKBONE_CONFIG
) ELSE (
    ECHO Packaged backbone installation does not exist. Run package_modules_and_configs for your platform first!
)