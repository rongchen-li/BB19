# ReadMe

This program replicates Table 1, Table 2, and Figure 1 in Ball and Brown (PBFJ, 2019). 

``I_Sample_Builder.py`` is a self-containing ``Python`` program that constructs the *annual* EPS dataset, the *daily* stock return dataset, as well as the keys to merge the two datasets.

``II_Main_Analyses.do`` is a ``STATA`` program that constructs the principal tables and figures of the replication.

``IIIA_Distribution.py`` and ``IIIB_Aggregation.py`` collectively perform parallel processing of the resampling computation. The following command is used to distribute the computing tasks on Columbia Business School Research Grid:

``grid_run --grid_submit=batch --grid_mem=20G --grid_SGE_TASK_ID=1-1250 --grid_ncpus=8 ./IIIA_Distribution.py``

Having obtained all the distributed computed results, ``IIIB_Aggregation.py`` can then be run to aggregate the resampling statistics.


```python

```
