# Oozie-SGE

## About

This is a repo for the Oozie-SGE plugin which can be used to submit Oozie workflow steps to SGE rather than to a Hadoop cluster as Map tasks.  At OICR, we use this to have a small Hadoop+Oozie scheduling cluster that then does workflow "heavy lifting" on a large SGE cluster.

## Future Work

* more scalable process for checking job status/outcome (right now it uses a lot of qstat/qccount calls)
* support for mixed workflows, e.g. I want to be able to mix jobs in a workflow, some use Hive/Pig/Impala/etc and are submitted to a Hadoop cluster while regular command line calls go to the SGE cluster

## See Also

Take a look at the main SeqWare project to see how our workflow API and associated tools work on top of this plugin http://seqware.io
