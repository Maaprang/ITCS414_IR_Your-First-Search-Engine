

import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

public class Index {

    // Term id -> (position in index file, doc frequency) dictionary
    private static Map<Integer, Pair<Long, Integer>> postingDict
            = new TreeMap<Integer, Pair<Long, Integer>>();
    // Doc name -> doc id dictionary
    private static Map<String, Integer> docDict
            = new TreeMap<String, Integer>();
    // Term -> term id dictionary
    private static Map<String, Integer> termDict
            = new TreeMap<String, Integer>();
    // Block queue
    private static LinkedList<File> blockQueue
            = new LinkedList<File>();

    // Total file counter
    private static int totalFileCount = 0;
    // Document counter
    private static int docIdCounter = 0;
    // Term counter
    private static int wordIdCounter = 0;
    // Index
    private static BaseIndex index = null;


    /*
     * Write a posting list to the given file
     * You should record the file position of this posting list
     * so that you can read it back during retrieval
     *
     * */
    private static void writePosting(FileChannel fc, PostingList posting)
            throws IOException {
        /*
         * TODO: Your code here
         *
         *
         */
        index.writePosting(fc, posting);


    }


    /**
     * Pop next element if there is one, otherwise return null
     *
     * @param iter an iterator that contains integers
     * @return next element or null
     */
    private static Integer popNextOrNull(Iterator<Integer> iter) {
        if (iter.hasNext()) {
            return iter.next();
        } else {
            return null;
        }
    }


    /**
     * Main method to start the indexing process.
     *
     * @param method        :Indexing method. "Basic" by default, but extra credit will be given for those
     *                      who can implement variable byte (VB) or Gamma index compression algorithm
     * @param dataDirname   :relative path to the dataset root directory. E.g. "./datasets/small"
     * @param outputDirname :relative path to the output directory to store index. You must not assume
     *                      that this directory exist. If it does, you must clear out the content before indexing.
     */
    public static int runIndexer(String method, String dataDirname, String outputDirname) throws IOException {
        /* Get index */
        String className = method + "Index";
        try {
            Class<?> indexClass = Class.forName(className);
            index = (BaseIndex) indexClass.newInstance();
        } catch (Exception e) {
            System.err
                    .println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
            throw new RuntimeException(e);
        }

        /* Get root directory */
        File rootdir = new File(dataDirname);
        if (!rootdir.exists() || !rootdir.isDirectory()) {
            System.err.println("Invalid data directory: " + dataDirname);
            return -1;
        }


        /* Get output directory*/
        File outdir = new File(outputDirname);
        if (outdir.exists() && !outdir.isDirectory()) {
            System.err.println("Invalid output directory: " + outputDirname);
            return -1;
        }

        /*	TODO: delete all the files/sub folder under outdir ** End **
         *
         */
        deleteFilesRecursively(outdir);


        if (!outdir.exists()) {
            if (!outdir.mkdirs()) {
                System.err.println("Create output directory failure");
                return -1;
            }
        }





        /* BSBI indexing algorithm */
        File[] dirlist = rootdir.listFiles();

        /* For each block */
        for (File block : dirlist) {
            File blockFile = new File(outputDirname, block.getName());
            //System.out.println("Processing block "+block.getName());
            blockQueue.add(blockFile);

            File blockDir = new File(dataDirname, block.getName());
            File[] filelist = blockDir.listFiles();

            TreeMap<Integer, TreeSet<Integer>> blockPosting = new TreeMap<>();

            /* For each file */
            for (File file : filelist) {
                ++totalFileCount;
                String fileName = block.getName() + "/" + file.getName();

                // use pre-increment to ensure docID > 0
                int docId = ++docIdCounter;
                docDict.put(fileName, docId);

                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.trim().split("\\s+");
                    for (String token : tokens) {
                        /*

                         * TODO: Your code here
                         *       For each term, build up a list of
                         *       documents in which the term occurs ** End **
                         */
                        int termID;
                        if (termDict.containsKey(token)) {
                            termID = termDict.get(token);

                        } else {
                            termID = ++wordIdCounter;
                            termDict.put(token, termID);
                        }

                        if (!blockPosting.containsKey(termID)) {
                            blockPosting.put(termID, new TreeSet<>());
                        }
                        blockPosting.get(termID).add(docId);

                    }
                }
                reader.close();
            }


            /* Sort and output */
            if (!blockFile.createNewFile()) {
                System.err.println("Create new block failure.");
                return -1;
            }

            RandomAccessFile bfc = new RandomAccessFile(blockFile, "rw");

            /*
             * TODO: Your code here
             *       Write all posting lists for all terms to file (bfc) **end**
             */


//            System.out.println("\nWriting " + block.getName());
            for (Map.Entry<Integer, TreeSet<Integer>> entry : blockPosting.entrySet()) {
                PostingList p = new PostingList(entry.getKey(), new ArrayList<>(entry.getValue()));
                if (!postingDict.containsKey(p.getTermId())) {
                    postingDict.put(p.getTermId(), new Pair<>(-1L, 0));
                }
                Pair<Long, Integer> integerPair = postingDict.get(p.getTermId());
                integerPair.setSecond(integerPair.getSecond() + p.getList().size());

//                System.out.println(p.getTermId() + " " + p.getList());
                index.writePosting(bfc.getChannel(), p);
            }
//            System.out.println("End Writing\n");

//            FileChannel fileChannel = bfc.getChannel();
//            fileChannel.position(0);
//            while (fileChannel.position() <= fileChannel.size() - 1){
//                PostingList readed = index.readPosting(fileChannel);
//                System.out.println("read " + readed.getTermId() + " " + readed.getList());
//            }
            bfc.close();
        }




        /* Required: output total number of files. */
        //System.out.println("Total Files Indexed: "+totalFileCount);

        /* Merge blocks */
        while (true) {
            if (blockQueue.size() <= 1)
                break;

            File b1 = blockQueue.removeFirst();
            File b2 = blockQueue.removeFirst();

            File combfile = new File(outputDirname, b1.getName() + "+" + b2.getName());
            if (!combfile.createNewFile()) {
                System.err.println("Create new block failure.");
                return -1;
            }

            RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
            RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
            RandomAccessFile mf = new RandomAccessFile(combfile, "rw");

            /*
             * TODO: Your code here
             *       Combine blocks bf1 and bf2 into our combined file, mf
             *       You will want to consider in what order to merge
             *       the two blocks (based on term ID, perhaps?).
             *
             */
            /*
             * read data from bf1 and bf2
             *  merging algorithm
             */
            ArrayList<PostingList> a = mergeList(bf1.getChannel(), bf2.getChannel(), index);

            for (PostingList pl : a) {
//                System.out.println(mf.getChannel().position());
//                System.out.println(pl.getTermId() + " -> " + mf.getChannel().position());
//                System.out.println(mf.getChannel().position());
                postingDict.get(pl.getTermId()).setFirst(mf.getChannel().position());
                index.writePosting(mf.getChannel(), pl);
            }
//            System.out.println();
            bf1.close();
            bf2.close();
            mf.close();
            b1.delete();
            b2.delete();
            blockQueue.add(combfile);
        }

        /* Dump constructed index back into file system */
        File indexFile = blockQueue.removeFirst();
        indexFile.renameTo(new File(outputDirname, "corpus.index"));

        BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(
                outputDirname, "term.dict")));
        for (String term : termDict.keySet()) {
            termWriter.write(term + "\t" + termDict.get(term) + "\n");
        }
        termWriter.close();

        BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(
                outputDirname, "doc.dict")));
        for (String doc : docDict.keySet()) {
            docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
        }
        docWriter.close();

        BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(
                outputDirname, "posting.dict")));

//        System.out.println(postingDict);
        for (Integer termId : postingDict.keySet()) {
            postWriter.write(termId + "\t" + postingDict.get(termId).getFirst()
                    + "\t" + postingDict.get(termId).getSecond() + "\n");
//            postingDict.get(termId);  /** test **/
//            index.writePosting();
        }
        postWriter.close();

        return totalFileCount;
    }

    public static void deleteFilesRecursively(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles()) {
                if (c.isDirectory()) {
                    deleteFilesRecursively(c);
                }
                c.delete();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        /* Parse command line */
        if (args.length != 3) {
            System.err
                    .println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
            return;
        }

        /* Get index */
        String className = "";
        try {
            className = args[0];
        } catch (Exception e) {
            System.err
                    .println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
            throw new RuntimeException(e);
        }

        /* Get root directory */
        String root = args[1];


        /* Get output directory */
        String output = args[2];
        runIndexer(className, root, output);
    }

    public static ArrayList<PostingList> mergeList(FileChannel fc1, FileChannel fc2, BaseIndex index) throws IOException {
        ArrayList<PostingList> result = new ArrayList<>();
        long i = 0, j = 0;
        while (i < fc1.size() && j < fc2.size()) {
            long old_position1 = fc1.position();
            long old_position2 = fc2.position();
            PostingList p1 = index.readPosting(fc1);
            PostingList p2 = index.readPosting(fc2);
            i = fc1.position();
            j = fc2.position();


            int c = p1.getTermId() - p2.getTermId();
            if (c > 0) {
                fc1.position(old_position1);
                result.add(p2);

            } else if (c < 0) {
                fc2.position(old_position2);
                result.add(p1);
            } else {
                result.add(mergePostingListPair(p1, p2));
            }
        }
        try {
            while (fc1.position() <= fc1.size() - 1) {
                result.add(index.readPosting(fc1));
            }

            while (fc2.position() <= fc2.size() - 1) {
                result.add(index.readPosting(fc2));
            }
        } catch (BufferUnderflowException e) {

        }


        result.sort(new Comparator<PostingList>() {
            @Override
            public int compare(PostingList postingList, PostingList t1) {
                return postingList.getTermId() - t1.getTermId();
            }
        });
        return result;
    }

    private static PostingList mergePostingListPair(PostingList p1, PostingList p2) {
        if (p1.getTermId() != p2.getTermId()) {
            return null;
        }

        TreeSet<Integer> a = new TreeSet<>(p1.getList());
        a.addAll(p2.getList());
        return new PostingList(p1.getTermId(), new ArrayList<>(a));

    }

}
