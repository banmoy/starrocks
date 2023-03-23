// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.fs.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;

public class ListPerf {

    private static HdfsFsManager hdfsFsManager = new HdfsFsManager();

    public static class LatencyStats {
        public long totalFiles = 0;
        public List<Long> values;
        private int threads = 0;

        public LatencyStats() {
            values = new ArrayList<>();
        }

        public void print(boolean summary) {
            Collections.sort(values);
            int size = values.size();
            long sum = values.stream().reduce(0L, Long::sum);
            int t = threads == 0 ? 1 : threads;
            float iops = t * size * 1000.f / sum;
            float avg = sum * 1.0f / size;
            long p00 = values.get(0);
            long p50 = values.get(size / 2);
            long p90 = values.get(size * 90 / 100);
            long p99 = values.get(size * 99 / 100);
            long p100 = values.get(size - 1);
            String sep = " - ";
            if (summary) {
                sep = "\n";
            }
            System.out.printf(
                    "IOPS: %.2f(T=%d, %d / %d / %dms)%sLatency: avg = %.2fms, min = %dms, p50 = %dms, p90 = %dms, p99 = %dms, max = %dms\n",
                    iops, totalFiles, t, size, sum, sep, avg, p00, p50, p90, p99, p100);
            if (summary) {
                String[] attrs = {"IOPS", "AVG", "MIN", "P50", "P90", "P99", "MAX"};
                System.out.println(String.join(",", attrs));
                System.out.printf("%.2f,%.2fms,%dms,%dms,%dms,%dms,%dms\n", iops, avg, p00, p50, p90, p99, p100);
            }
        }

        public void merge(LatencyStats stats) {
            threads += 1;
            values.addAll(stats.values);
        }
    }

    private static int listGlob(FileSystem fileSystem, Path pathPattern) throws Exception {
        FileStatus[] files = fileSystem.globStatus(pathPattern);
        return files.length;
    }

    private static int listStatus(FileSystem fileSystem, Path pathPattern) throws Exception {
        FileStatus[] statuses = fileSystem.listStatus(pathPattern);
        return statuses.length;
    }

    private static int listFiles(FileSystem fileSystem, Path pathPattern) throws Exception {
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(pathPattern, true);
        int count = 0;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
        }
        return count;
    }

    private static int list(String mode, FileSystem fileSystem, Path pathPattern) throws Exception {
        switch (mode.toUpperCase()) {
            case "GLOBSTATUS":
                return listGlob(fileSystem, pathPattern);
            case "LISTFILES":
                return listFiles(fileSystem, pathPattern);
            case "LISTSTATUS":
                return listStatus(fileSystem, pathPattern);
            default:
                throw new UnsupportedOperationException("Unsupported mode " + mode);
        }
    }

    public static class Runner implements Runnable {
        public LatencyStats stats;
        String path;
        int repeat;
        String mode;
        Map<String, String> properties;
        HdfsFs hdfsFs;
        WildcardURI pathUri;

        public Runner(String path, int repeat, String mode, Map<String, String> properties) throws Exception {
            this.path = path;
            this.repeat = repeat;
            this.mode = mode;
            this.properties = properties;
            this.hdfsFs = hdfsFsManager.getFileSystem(path, properties, null);
            this.pathUri = new WildcardURI(path);
            this.stats = new LatencyStats();
        }

        @Override
        public void run() {
            FileSystem fileSystem = hdfsFs.getDFSFileSystem();
            Path pathPattern = new Path(pathUri.getPath());
            while (repeat > 0) {
                repeat--;
                long ss = System.currentTimeMillis();
                int count;
                try {
                    count = list(mode, fileSystem, pathPattern);
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
                long ee = System.currentTimeMillis();
                long t = ee - ss;
                stats.values.add(t);
                stats.totalFiles += count;
            }
            stats.print(false);
        }
    }

    public static void main(String[] args) throws Exception {
        int num_threads = 16;
        int repeat = 1;
        String ak = null;
        String sk = null;
        String endpoint = null;
        String path = null;
        String mode = null;
        for (int i = 0; i < args.length; ) {
            String opt = args[i];
            String value = null;
            if ((i + 1) < args.length) {
                value = args[i + 1];
            }
            if (opt.equals("--repeat")) {
                repeat = Integer.parseInt(value);
            } else if (opt.equals("--thread")) {
                num_threads = Integer.parseInt(value);
            } else if (opt.equals("--path")) {
                path = value;
            } else if (opt.equals("--ak")) {
                ak = value;
            } else if (opt.equals("--sk")) {
                sk = value;
            } else if (opt.equals("--endpoint")) {
                endpoint = value;
            } else if (opt.equals("--mode")) {
                mode = value;
            } else {
                i -= 1;
            }
            i += 2;
        }

        System.out.printf(
                "--mode %s --thread %d --repeat %d --ak %s --sk %s --endpoint %s --path %s \n",
                mode, num_threads, repeat, ak, sk, endpoint, path);
        if (path == null) {
            System.out.println("path is null");
            return;
        }

        System.out.printf("==========  testing  ==========\n");
        Map<String, String> properties = new HashMap<>();
        properties.put(AWS_S3_ACCESS_KEY, ak);
        properties.put(AWS_S3_SECRET_KEY, sk);
        properties.put(AWS_S3_ENDPOINT, endpoint);

        List<Thread> threads = new ArrayList<>();
        List<Runner> runners = new ArrayList<>();
        LatencyStats stats = new LatencyStats();
        for (int i = 0; i < num_threads; i++) {
           Runner r = new Runner(path, repeat, mode, properties);
            runners.add(r);
            Thread t = new Thread(r);
            threads.add(t);
            t.start();
        }
        for (int i = 0; i < num_threads; i++) {
            threads.get(i).join();
            stats.merge(runners.get(i).stats);
        }
        System.out.print("==========  summary  ==========\n");
        stats.print(true);
    }
}
