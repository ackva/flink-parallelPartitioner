    package org.myorg.quickstart.utils;
    /*
     * Copyright (C) 2017 codevscolor
     *
     * Licensed under the Apache License, Version 2.0 (the "License");
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     *
     *      http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */
    import java.io.File;
    import java.io.IOException;
    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.List;

    /**
     * Example class
     */
    public class VertexCutsMultipleJobs {
        //utility method to print a string
        static void print(String value) {
            System.out.println(value);
        }
        /**
         * Method to sort all files and folders in a directory
         *
         * @param dirName : directory name
         * @return : No return value. Sort and print out the result
         */
        private static void sortAll(String dirName) throws IOException {
            File directory = new File(dirName);
            File[] filesArray = directory.listFiles();
            //sort all files
            Arrays.sort(filesArray);
            //print the sorted values
            for (File file : filesArray) {
                if (file.isFile()) {
                    print("File : " + file.getName());
                } else if (file.isDirectory() && file.getName().contains("job_")) {
                    File[] jobOutputFiles = file.listFiles();
                    System.out.println( file.getName() + "'s files: ");
                    List<File> fileList = new ArrayList<>();
                    int parallelism = 0;
                    for (File f : jobOutputFiles) {
                        fileList.add(f);
                        System.out.println("File: " + f.getName());
                        parallelism=+1;
                    }
                    double vertexCut = new VertexCut(parallelism).calculateVertexCut(fileList);
                    System.out.println("Vertex Cut: " + vertexCut);
                    System.out.println(" --- ");
                    //print("Directory : " + file.getName());
                } else {
                    print("No Job Output Directory found : " + file.getName());
                }
            }
        }
        public static void main(String[] args) throws IOException {
            //sortAll("C://Programs/");
            sortAll(args[0]);
        }
    }