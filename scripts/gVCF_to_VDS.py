#!/usr/bin/env python3
import os
import argparse
import pyspark
import hail as hl
from hail.utils.java import Env
from pyspark import SparkConf, SparkContext
from hail.vds.combiner import new_combiner

def main():
    p = argparse.ArgumentParser(description="Merge gVCFs into a VDS with Hail")
    
    # Resources & tuning
    p.add_argument('--threads',        type=int,   default=50,
                   help="Number of cores (e.g. PBS -l select=1:ncpus=XX)")
    p.add_argument('--mem',            type=str,   default='16g',
                   help="Memory per JVM (e.g. 16g)")
    p.add_argument('--branch-factor',  type=int,   default=15,
                   help="Combiner branch_factor")
    p.add_argument('--batch-size',     type=int,   default=10,
                   help="gVCF batch size")
    p.add_argument('--target-records', type=int,   default=1_000_000,
                   help="Variants per partition")
    
    # Paths _you_ will change per run
    p.add_argument('--log-path',       type=str, required=True,
                   help="Where to write hail log")
    p.add_argument('--combiner-tmp',   type=str, required=True,
                   help="Combiner temporary directory")
    p.add_argument('--gvcf-list',      type=str, required=True,
                   help="Text file with one gVCF path per line")
    p.add_argument('--output-vds',     type=str, required=True,
                   help="Where to write the merged VDS")
    
    # Paths you keep static:
    p.add_argument('--spark-local',    type=str,
                   default='/rds/general/project/lms-ware-analysis/ephemeral/HAIL/spark_local')
    p.add_argument('--tmp-dir',        type=str,
                   default='/rds/general/project/lms-ware-analysis/ephemeral/HAIL/hail_tmp')
    
    args = p.parse_args()

    # discover the hail-all-spark.jar for *this* env:
    hail_pkg_dir = os.path.dirname(hl.__file__)
    hail_jar = os.path.join(hail_pkg_dir, 'backend', 'hail-all-spark.jar')
    if not os.path.exists(hail_jar):
        raise FileNotFoundError(f"hail-all-spark.jar not found at {hail_jar}")

    # make combiner tmp if not exist
    os.makedirs(args.combiner_tmp, exist_ok=True)
    plan = os.path.join(args.combiner_tmp, 'combiner_plan.json')

    # build SparkConf
    conf = SparkConf().setAll([
        ('spark.master',               f'local[{args.threads}]'),
        ('spark.app.name',             'HailCombiner'),
        ('spark.jars',                 hail_jar),
        ('spark.driver.extraClassPath',   hail_jar),
        ('spark.executor.extraClassPath', hail_jar),
        ('spark.serializer',           'org.apache.spark.serializer.KryoSerializer'),
        ('spark.kryo.registrator',     'is.hail.kryo.HailKryoRegistrator'),
        ('spark.driver.memory',        args.mem),
        ('spark.executor.memory',      args.mem),
        ('spark.local.dir',            args.spark_local),
    ])

    # init Spark + Hail
    sc = SparkContext(conf=conf)
    hl.init(sc=sc,
            tmp_dir=args.tmp_dir,
            log=args.log_path)
    
    # now read gVCF list and run combiner
    with open(args.gvcf_list) as f:
        gvcfs = [l.strip() for l in f if l.strip()]
    print(f"Found {len(gvcfs)} gVCFs")

    combiner = new_combiner(
        gvcf_paths=gvcfs,
        output_path=args.output_vds,
        temp_path=args.combiner_tmp,
        save_path=plan,
        reference_genome='GRCh38',
        use_genome_default_intervals=True,
        branch_factor=args.branch_factor,
        gvcf_batch_size=args.batch_size,
        target_records=args.target_records
    )
    combiner.run()
    
    hl.stop()

if __name__ == '__main__':
    main()
