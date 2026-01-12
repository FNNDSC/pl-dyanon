# A dynamic anonymization ChRIS plugin

[![Version](https://img.shields.io/docker/v/fnndsc/pl-dyanon?sort=semver)](https://hub.docker.com/r/fnndsc/pl-dyanon)
[![MIT License](https://img.shields.io/github/license/fnndsc/pl-dyanon)](https://github.com/FNNDSC/pl-dyanon/blob/main/LICENSE)
[![ci](https://github.com/FNNDSC/pl-dyanon/actions/workflows/ci.yml/badge.svg)](https://github.com/FNNDSC/pl-dyanon/actions/workflows/ci.yml)

`pl-dyanon` is a [_ChRIS_](https://chrisproject.org/)
_ds_ plugin which takes CSV-based DICOM selection and rule definition files as input and
produces dynamically anonymized DICOM studies as output. The plugin can optionally push
results to Orthanc, a remote PACS, or downstream ChRIS workflows.

## Abstract

Medical imaging workflows frequently require flexible, rule-driven anonymization that can
adapt dynamically to different studies, projects, or data-sharing requirements.
`pl-dyanon` enables scalable, bulk DICOM anonymization driven by external metadata,
CSV inputs, and configurable tag-preservation rules.

The plugin integrates with the ChRIS ecosystem, Orthanc servers, and optional PACS
endpoints. It supports parallel execution, selective DICOM filtering, image-count–based
rules, and optional email notifications upon completion.


## Installation

`pl-dyanon` is a _[ChRIS](https://chrisproject.org/) plugin_, meaning it can
run from either within _ChRIS_ or the command-line.

## Local Usage

To get started with local command-line usage, use [Apptainer](https://apptainer.org/)
(a.k.a. Singularity) to run `pl-dyanon` as a container:

```shell
apptainer exec docker://fnndsc/pl-dyanon dyanon [--args values...] input/ output/
```

To print its available options, run:

```shell
apptainer exec docker://fnndsc/pl-dyanon dyanon --help
```

## Examples

`dyanon` requires two positional arguments: a directory containing
input data, and a directory where to create output data.
First, create the input directory and move input data into it.

```shell
mkdir incoming/ outgoing/
mv some.dat other.dat incoming/
apptainer exec docker://fnndsc/pl-dyanon:latest dyanon [--args] incoming/ outgoing/
```

## Development

Instructions for developers.

### Building

Build a local container image:

```shell
docker build -t localhost/fnndsc/pl-dyanon .
```

### Running

Mount the source code `dyanon.py` into a container to try out changes without rebuild.

```shell
docker run --rm -it --userns=host -u $(id -u):$(id -g) \
    -v $PWD/dyanon.py:/usr/local/lib/python3.12/site-packages/dyanon.py:ro \
    -v $PWD/in:/incoming:ro -v $PWD/out:/outgoing:rw -w /outgoing \
    localhost/fnndsc/pl-dyanon dyanon /incoming /outgoing
```

### Testing

Run unit tests using `pytest`.
It's recommended to rebuild the image to ensure that sources are up-to-date.
Use the option `--build-arg extras_require=dev` to install extra dependencies for testing.

```shell
docker build -t localhost/fnndsc/pl-dyanon:dev --build-arg extras_require=dev .
docker run --rm -it localhost/fnndsc/pl-dyanon:dev pytest
```

## Release

Steps for release can be automated by [Github Actions](.github/workflows/ci.yml).
This section is about how to do those steps manually.

### Increase Version Number

Increase the version number in `setup.py` and commit this file.

### Push Container Image

Build and push an image tagged by the version. For example, for version `1.2.3`:

```
docker build -t docker.io/fnndsc/pl-dyanon:1.2.3 .
docker push docker.io/fnndsc/pl-dyanon:1.2.3
```

### Get JSON Representation

Run [`chris_plugin_info`](https://github.com/FNNDSC/chris_plugin#usage)
to produce a JSON description of this plugin, which can be uploaded to _ChRIS_.

```shell
docker run --rm docker.io/fnndsc/pl-dyanon:1.2.3 chris_plugin_info -d docker.io/fnndsc/pl-dyanon:1.2.3 > chris_plugin_info.json
```

Intructions on how to upload the plugin to _ChRIS_ can be found here:
https://chrisproject.org/docs/tutorials/upload_plugin

