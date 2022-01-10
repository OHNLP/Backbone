REM Runs the specified backbone pipeline on Google Cloud Platform using Dataflow
REM For a full description of options available here, please consult https://beam.apache.org/documentation/runners/dataflow/

cd %~dp0

SET BACKBONE_CONFIG=NAME_OF_CONFIG_TO_USE.json
SET GCP_PROJECT_ID=YOUR_GCP_PROJECT_ID
SET GCP_REGION=YOUR_GOOGLE_CLOUD_REGION

IF EXIST bin\Backbone-Core-GCP-Packaged.jar (
    java -cp bin\Backbone-Core-GCP-Packaged.jar org.ohnlp.backbone.core.BackboneRunner\
    --config=%BACKBONE_CONFIG% \
    --runner=DataflowRunner \
    --project=%GCP_PROJECT_ID% \
    --region=%GCP_REGION%
) ELSE (
    ECHO Packaged backbone installation does not exist. Run package_modules_and_configs for your platform first!
)