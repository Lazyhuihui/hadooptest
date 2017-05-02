package com.huihui.mr;

import java.io.BufferedReader;

import java.io.IOException;

import java.io.InputStream;

import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

 

/**

 *

 * The utilities to operate file on hadoop hdfs.

 *

 * @author luolihui 2011-07-18

 *

 */

public class DFSOperator {

        

         private static final String ROOT_PATH = "hdfs:///";

         private static final int BUFFER_SIZE = 4096;

        

         /**

          * construct.

          */

         public DFSOperator(){}

 

     /**

     * Create a file on hdfs.The root path is /.<br>

     * for example: DFSOperator.createFile("/lory/test1.txt", true);

          * @param path  the file name to open

          * @param overwrite if a file with this name already exists, then if true, the file will be

          * @return true if delete is successful else IOException.

          * @throws IOException

          */

         public static boolean createFile(String path, boolean overwrite) throws IOException

         {

                   //String uri = "hdfs://192.168.1.100:9000";

                   //FileSystem fs1 = FileSystem.get(URI.create(uri), conf); 

 

                   Configuration conf = new Configuration();

                   conf.set("fs.defaultFS", "hdfs://localhost:9000"); 
                   FileSystem fs = FileSystem.get(conf);

                   Path f = new Path(path); 

                   fs.create(f, overwrite);

                   fs.close();

                   return true;

         }

        

    /**

     * Delete a file on hdfs.The root path is /. <br>

     * for example: DFSOperator.deleteFile("/user/hadoop/output", true);

     * @param path the path to delete

     * @param recursive  if path is a directory and set to true, the directory is deleted else throws an exception. In case of a file the recursive can be set to either true or false.

     * @return true if delete is successful else IOException.

     * @throws IOException

     */

         public static boolean deleteFile(String path, boolean recursive) throws IOException

         {

                   //String uri = "hdfs://192.168.1.100:9000";

                   //FileSystem fs1 = FileSystem.get(URI.create(uri), conf); 

                  

                   Configuration conf = new Configuration();
                   conf.set("fs.defaultFS", "hdfs://localhost:9000");
                   FileSystem fs = FileSystem.get(conf);

                   Path f = new Path(path); 

                   fs.delete(f, recursive);

                   fs.close();

                   return true;

         }

        

         /**

          * Read a file to string on hadoop hdfs. From stream to string. <br>

          * for example: System.out.println(DFSOperator.readDFSFileToString("/user/hadoop/input/test3.txt"));

          * @param path the path to read

          * @return true if read is successful else IOException.

          * @throws IOException

          */

         public static String readDFSFileToString(String path) throws IOException

         {

                   Configuration conf = new Configuration();
                   conf.set("fs.defaultFS", "hdfs://localhost:9000");
                   FileSystem fs = FileSystem.get(conf);

                   Path f = new Path(path);

                   InputStream in = null;

                   String str = null;

                   StringBuilder sb = new StringBuilder(BUFFER_SIZE);

                   if (fs.exists(f))

                   {

                            in = fs.open(f);

                            BufferedReader bf = new BufferedReader(new InputStreamReader(in));

                           

                            while ((str = bf.readLine()) != null)

                            {

                                     sb.append(str);

                                     sb.append("\n");

                            }

                           

                            in.close();

                            bf.close();

                            fs.close();

                            return sb.toString();

                   }

                   else

                   {

                            return null;

                   }

                  

         }

         /**

          * Write string to a hadoop hdfs file. <br>

          * for example: DFSOperator.writeStringToDFSFile("/lory/test1.txt", "You are a bad man.\nReally!\n");

          * @param path the file where the string to write in.

          * @param string the context to write in a file.

          * @return true if write is successful else IOException.

          * @throws IOException

          */

         public static boolean writeStringToDFSFile(String path, String string) throws IOException

         {

                   Configuration conf = new Configuration();
                   conf.set("fs.defaultFS", "hdfs://localhost:9000");
                   FileSystem fs = FileSystem.get(conf);

                   FSDataOutputStream os = null;

                   Path f = new Path(path);

                   os = fs.create(f,true);

                   os.writeBytes(string);

                  

                   os.close();

                   fs.close();

                   return true;

         }

         
         public static boolean catFile(String path) throws IOException

         {

                   //String uri = "hdfs://192.168.1.100:9000";


 

                   Configuration conf = new Configuration();

                   conf.set("fs.defaultFS", "hdfs://localhost:9000"); 
                   FileSystem fs = FileSystem.get(URI.create(path), conf); 
                   //FileSystem fs = FileSystem.get(conf);
                   InputStream in=null;
                   try {
					in =fs.open(new Path(path));
					IOUtils.copyBytes(in, System.out, 4069,false);
				} catch (Exception e) {
					// TODO: handle exception
				}finally {
					IOUtils.closeStream(in);
				}
                   

                   return true;

         }
 

         public static void main(String[] args)

         {

                   try {

                            DFSOperator.createFile("/user/hdfs/log_kpi/log/log1.txt", true);

                           // DFSOperator.deleteFile("/lory", true);
                            String line = "222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36\"\n";
                            String line1 = "222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\" \"Mozilla/6.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome1/28.0.1547.66 Safari/537.36\"\n";
                           
                           // DFSOperator.writeStringToDFSFile("/user/hdfs/log_kpi/log1.txt", "You are a bad man.\nReally?\n");
                            DFSOperator.writeStringToDFSFile("/user/hdfs/log_kpi/log/log1.txt", line+line1+line+line1);

                            //System.out.println(DFSOperator.readDFSFileToString("/lory/test1.txt"));
                          //  DFSOperator.readDFSFileToString("");
                	 //  DFSOperator.catFile("/lory/test1.txt"); 

                   } catch (IOException e) {

                            // TODO Auto-generated catch block

                            e.printStackTrace();

                   }

                   System.out.println("===end===");

         }

}
