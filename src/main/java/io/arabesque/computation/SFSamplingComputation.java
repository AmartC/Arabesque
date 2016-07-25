package io.arabesque.computation;

import io.arabesque.conf.Configuration;
import io.arabesque.embedding.BasicEmbedding;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;

import java.util.Random;

/**
 * Created by carlos on 11/05/16.
 */

/**
 * Created by carlos on 11/05/16.
 */
public abstract class SFSamplingComputation<E extends BasicEmbedding> extends BasicComputation<E> {

    protected Random r;
    protected int samplesize;
    protected int maxsize;

    protected static final String SAMPLESIZE = "arabesque.sampling.samplesize";
    protected static final String MAXSIZE = "arabesque.sampling.maxsize";
    protected static final int SAMPLESIZE_DEFAULT = 100;
    protected static final int MAXSIZE_DEFAULT = 4;

    @Override
    public void init() {
        super.init();
        //r = new Random(getPartitionId()); // TODO: custom seed?
        r = new Random(System.currentTimeMillis()); // TODO: custom seed?
        samplesize = Configuration.get().getInteger(SAMPLESIZE, SAMPLESIZE_DEFAULT);
        maxsize = Configuration.get().getInteger(MAXSIZE, MAXSIZE_DEFAULT);
    }

    @Override
    protected void doModifyFilter(int wordId) {
        if (wordId >= 0)
            doModifyFilterWord (wordId);
        else
            doModifyFilterRandom (-1 * wordId, getInitialNumWords());
    }

    private void doModifyFilterRandom(int numWords, int upperBound) {
        assert (upperBound > 0);
        for (int i = 0; i < numWords; i++)
            doModifyFilterWord (r.nextInt(upperBound));
    }

    private void doModifyFilterWord(int wordId) {

        //System.out.println("Orig 1: " + currentEmbedding.toOutputString());
        currentEmbedding.addWord(wordId);

        //System.out.println("Alt 1: " + currentEmbedding.toOutputString());
        //remove one word at random, but keeping connected
        int rmWordId = -1;
        if (currentEmbedding.getNumWords()> maxsize) {
            IntCollection contractions = currentEmbedding.getContractibleWordIds();
            //System.out.println("Valid contractions: " + contractions);
            contractions.removeInt(wordId);
            int[] contractionsArray = contractions.toIntArray();
            int i = r.nextInt(contractions.size());
            //System.out.println("Selected pos: " + i);
            currentEmbedding.removeWord(contractionsArray[i]);
            rmWordId = contractionsArray[i];
        }
        //System.out.println("Alt 2: " + currentEmbedding.toOutputString());

        //process modification
        if (shouldModify(currentEmbedding)) {
            underlyingExecutionEngine.processModification(currentEmbedding);
        }
        numChildrenEvaluated++;
        process(currentEmbedding);

        //restore the original emb.
        if (rmWordId!=-1)
            currentEmbedding.addWord(rmWordId);
        currentEmbedding.removeWord(wordId);
        //System.out.println("Orig 2: " + currentEmbedding.toOutputString());
    }

    private int modificationsSize(E embedding, int[] modifications) {
        int i = 0;

        if (modifications.length == 0) return 0;
        else if (modifications[i] == -1) // all words
            return embedding.getTotalNumWords();
        else if (modifications[i] < 0)
            return (-1) * modifications[i];

        return modifications.length;
    }

    protected int nextModification(E embedding, int[] modifications, int upperBound) {
        int rdIdx = r.nextInt(upperBound);
        int i = 0, j = 0;

        if (modifications[i] == -1) {
                return rdIdx;
        } else if (modifications[i] < 0) {
            int nextRd = r.nextInt (getInitialNumWords());
            return nextRd;
        } else {
            int weights[] = new int[upperBound];
            int accW = 0;
            for (i = 0; i < upperBound;++i) {
                weights[i] = getNumberOfModificationsByAddWord(embedding,modifications[i]);
                accW += weights[i];
            }
            assert (accW > 0);
            if (accW>0) {
                int rValue = r.nextInt(accW);
                accW=0;
                for (i = 0; i < upperBound; ++i) {
                    accW += weights[i];
                    if (rValue < accW) return modifications[i];
                }
            }
        }
        throw new RuntimeException ("Upper bound is greater than modifications");
    }

    @Override
    public void filter(E existingEmbedding, IntCollection modificationPoints) {

        int[] modificationsArray = modificationPoints.toIntArray();
        int previousDegree = modificationsSize (existingEmbedding, modificationsArray);
        if (previousDegree == 0) {
            existingEmbedding.reset();
            return;
        }

        int nextModification = nextModification(existingEmbedding,modificationsArray,previousDegree);

        modificationPoints.clear();
        modificationPoints.add(nextModification);
    }

    @Override
    public boolean filter(E existingEmbedding, int newWord) {
        return true;
    }

    @Override
    public void handleNoModifications(E embedding) {
        if (filter(embedding)) {
            if (shouldModify(embedding)) {
                underlyingExecutionEngine.processModification(embedding);
            }
            numChildrenEvaluated++;
            process(embedding);
        }
    }
    @Override
    protected IntArrayList getInitialExtensions() {

        IntArrayList initialExtensions = new IntArrayList();
        int numInitialWords = getInitialNumWords();

        //int i = 0;
        //while (i<samplesize/getNumberPartitions()) {
        //	initialExtensions.add(r.nextInt(numInitialWords));
        //	i++;
        //}
        //
        initialExtensions.add(-1 * (samplesize/getNumberPartitions()));

        return initialExtensions;
    }

    @Override
    protected IntCollection getPossibleModifications(E embedding) {
        IntCollection possibleModifications = getPossibleExtensions(embedding);
        return possibleModifications;
    }

    protected int getNumberOfEmbeddingsNeighbors(E embedding) {
        IntCollection modificationPoints = embedding.getExtensibleWordIds();
        int[] modificationsArray = modificationPoints.toIntArray();
        int previousDegree = modificationsSize (embedding, modificationsArray);

        int degree = 0;
        for (int i = 0; i < previousDegree;++i) {
            degree+=getNumberOfModificationsByAddWord(embedding,modificationsArray[i]);
        }
        return degree;
    }

    private int getNumberOfModificationsByAddWord(E embedding, int wordId) {
        embedding.addWord(wordId);
        IntCollection contractions = embedding.getContractibleWordIds();
        //System.out.println("Possible contractions with mod. " + modifications[i] + " : " + contractions);
        embedding.removeWord(wordId);

        contractions.removeInt(wordId);
        return contractions.size();

    }
}


