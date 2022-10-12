# OHNLP Backbone

## Introduction

Backbone aims to simplify scalable ETL (Extract-Transform-Load) processes by transforming such 
operations into a sequence of simple user-accessible JSON configurations, with a particular
focus on Healthcare NLP-related tasks.

An example ETL task supported by Backbone common to healthcare NLP is
1. \[Extract]: Extracting clinical narratives from Epic Clarity
2. \[Transform]:
    * Convert extracted RTFs to plaintext
    * Run a [MedTagger](https://github.com/OHNLP/MedTagger) ruleset on the resulting plaintext
    * Transform the resulting NLP artifacts into some standard format for NLP output, such as the OHDSI CDM's NOTE_NLP 
      specification
3. \[Load]: Write the resulting standardized NLP artifacts back to a SQL database, such as an OHDSI CDM install  

By wrapping [Apache Beam](https://beam.apache.org/), any created pipelines are runnable out-of-the-box across
a wide variety of environments with minimal additional configuration, from a local computer to distributed environments
such as Apache Spark or Google Cloud Dataflow, ensuring scalability and timely completion regardless of input data size

## System Requirements
*	Data Source/Output: Any of the following
    *	JDBC-compatible data source/output location (with OMOP schema preferred, but not required)
    *  Google Bigquery
    *	Apache Hive/HCatalog
    *	MongoDB v6.0+ with allowDiskUseByDefault set to true if using as data source
    *	Supported, but extremely not recommended for production workloads: Elasticsearch v6+, File based I/O (CSV or JSONL in/out, or Parquet Format with predefined schema)
*	Computational Needs: One of the following. JDK8+ required, 11+ heavily recommended (has performance and stability implications) regardless of option chosen. 
    *	Local/non-cluster-based computation: Unix/Linux based machine with (# CPU cores available on machine) * .8 >= # CPU cores desired. Must be able to change open file limit via ulimit command for production workloads
    *	Spark cluster (either via yarn or local computation) 
    *	Flink Cluster (either local/via yarn, and/or AWS default setup)
    *	GCP Dataflow
 
Whichever computational system chosen MUST be able to access your data input/output locations (i.e., check firewall settings etc)
 
For computational needs: Note that core count can vary depending on workload/how fast you care about things processing. A good rule of thumb is 3 documents/second/core for average workloads, but can vary. For maximum stability (i.e., with maximum-size UMLS-based dictionary), 2GB RAM/CPU core is recommended although most workloads will use far less

   
## Getting Started

To begin, download the [latest release](https://github.com/OHNLP/Backbone/releases/latest) and extract the resulting 
zip file to a location  which we will hereafter refer to as `BACKBONE_ROOT`.

The resulting directory structure should resemble the following:

```
[BACKBONE_ROOT]
    /[bin]
    /[configs]
    /[modules]
    /[resources]
    /package_modules_and_configs.{bat|sh}
    /run_pipeline_{environment}.{bat|sh} 
```
 
Before running a pipeline, we must first do some [module setup and configuration](#configuration). 
When complete, run the `package_modules_and_configs` appropriate to your system (`bat` for windows, `sh` for unix/linux/mac)

This step must be repeated every time any resources located in `[configs]`, `[modules]`, or `[resources]` changes

To run the resulting pipeline, edit the appropriate `run_pipeline_{environment}.{bat|sh}` file with your settings, and 
then simply execute when complete.

## Pipeline Configuration
Configurations, located in `BACKBONE_ROOT/configs`, define precisely what actions should be taken during an ETL task. For 
convenience, several example pipeline configurations are supplied within the aforementioned folder in the format \[task name]\_\[data_source]\_\[data_target].json

Fundamentally, a configuration is a json file bearing the following structure:

```json
{
  "id": "An identifier uniquely identifying this pipeline",
  "description": "Some human readable description for this pipeline",
  "pipeline": [
    {
      "clazz": "fully.qualified.class.name.of.pipeline.Component",
      "config": {
        "some_config_key": "some config value for this specific pipeline component"
      }   
    },
    ...
    {
      "clazz": "some.other.fully.qualified.class.name.of.pipeline.Component",
      "config": {
        "some_config_key": "some config value for this specific pipeline component"
      }   
    }   
  ]
}
```

A pipeline is then created by going in-order through the pipeline array. The first component is expected to be an Extract
, the final component is expected to be a Load, and intermediate components are expected to be transforms.

Accepted configuration values for each component are defined by the component themselves, consult their documentation
for a full list of available parameters.

When a configuration is changed or created, it must be stored within the `BACKBONE_ROOT/configs` folder and
`package_modules_and_configs` must be run to update the executed binary. 

The pipeline to run can then be selected by editing the appropriate `run_pipeline_*` script and changing the 
`BACKBONE_CONFIG` variable to match the desired configuration

## Modules

The Backbone project itself does not supply a comprehensive coverage of NLP tasks, nor is it intended to.
Indeed, Backbone is merely intended to provide a framework by which such components can be easily intertwined and configured
to run as part of an ETL pipeline at scale.

As such, 3rd party libraries supply their own pipeline components that can be installed as modules to supply their functionality
as part of a backbone module. 

Backbone modules are stored in `BACKBONE_ROOT/modules` and can be installed once placed using `package_modules_and_configs` 
much like updating configurations.  

Modules are expected to supply the fully qualified class names and associated pipeline configuration settings as part of 
their documentation.

Additionally, modules may require external file resources, which can be placed in the `BACKBONE_ROOT/modules` folder, 
and then similarly packaged using `package_modules_and_configs`

### Official OHNLP Supported Backbone Modules 
Below is a listing of OHNLP Backbone Compatible Projects - Please Consult their Documentation for Instructions 
on their use as part of your pipelines.

* [MedTagger](https://github.com/OHNLP/MedTagger) - Run Generic User-Defined Clinical NLP On Text
* [MedXN](https://github.com/OHNLP/MedXN) - Run the MedXN Drug Extraction Pipeline 
* [AEGIS](https://github.com/OHNLP/AEGIS) - Run AEGIS Sentinel Syndromic Surveillance Model Training and Evaluation  
