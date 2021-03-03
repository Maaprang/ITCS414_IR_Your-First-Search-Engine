import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;


public class BasicIndex implements BaseIndex {

    @Override
    public PostingList readPosting(FileChannel fc) {
        /*
         * TODO: Your code here
         *       Read and return the postings list from the given file. **End **
         *
         */

        int termId = 0, docFreq = 0;
        ByteBuffer headerBB = ByteBuffer.allocate(2 * Integer.BYTES);
        try {
            fc.read(headerBB);
            headerBB.flip();
            termId = headerBB.getInt();
            docFreq = headerBB.getInt();
        } catch (IOException e) {
            e.printStackTrace();
        }

//		System.out.println("\nTermId="+ termId + "\nDocFreq=" + docFreq);
        ArrayList<Integer> docIdArrayList = new ArrayList<>();
        int bufferSize = findTheMaxFactor(docFreq);
//        System.out.println("old " + docFreq + "\tBufferSize=" + bufferSize);
        ByteBuffer docIdBuffer = ByteBuffer.allocate(bufferSize * Integer.BYTES);
        for (int i = 0; i < docFreq / bufferSize; i++) {
            try {
                fc.read(docIdBuffer);
                docIdBuffer.flip();
                try {
                    for (int j = 0; j < bufferSize; j++) {
                        docIdArrayList.add(docIdBuffer.getInt());
//                        System.out.println(docIdArrayList.get(i));
                    }
                } catch (BufferUnderflowException e) {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                docIdBuffer.clear();

            }
        }
        return new PostingList(termId, docIdArrayList);
    }

    @Override
    public void writePosting(FileChannel fc, PostingList p) {
        /*
         * TODO: Your code here
         *       Write the given postings list to the given file. **End**
         *
         */
        ArrayList<Integer> list = new ArrayList<>();
        list.add(p.getTermId());
        list.add(p.getList().size());
        list.addAll(p.getList());


        ByteBuffer buffer = ByteBuffer.allocate(list.size() * Integer.BYTES);
        for (int data : list) {
//            System.out.println("Added " + data);
            buffer.putInt(data);
        }
        try {
			buffer.flip();
			fc.write(buffer);
		} catch (IOException e) {
//			e.printStackTrace();
        }
    }

    /* ADD to calculate the Max index for postingList buffer*/
    static int findTheMaxFactor(int number) {
//        System.out.println(number);
        if (number == 1){
            return number;
        }
        int n = number;
        int max = Integer.MIN_VALUE;
        for (int i = 2; i <= n; i++) {
            while (n % i == 0) {
                max = Math.max(max, i);
                n /= i;
            }
        }
        return max;
    }
}
