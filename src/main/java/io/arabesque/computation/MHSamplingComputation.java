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
public abstract class MHSamplingComputation<E extends BasicEmbedding> extends BasicComputation<E> {

    private Random r;
    protected int samplesize;

    protected static final String SAMPLESIZE = "arabesque.motif.samplesize";
    protected static final int SAMPLESIZE_DEFAULT = 100;

    @Override
    public void init() {
        super.init();
        r = new Random(getPartitionId()); // TODO: custom seed?
        samplesize = Configuration.get().getInteger(SAMPLESIZE, SAMPLESIZE_DEFAULT);
    }

    @Override
    protected void doModifyFilter(int wordId) {
        if (wordId >= 0)
            doModifyFilterWord (wordId);
        else
            doModifyFilterRandom (-1 * wordId, getInitialNumWords());
    }

    private void doModifyFilterRandom(int numWords, int upperBound) {
        for (int i = 0; i < numWords; i++)
            doModifyFilterWord (r.nextInt(upperBound));
    }

    private void doModifyFilterWord(int wordId) {

        //if expansion
        if (!currentEmbedding.existWord(wordId)) {
            currentEmbedding.addWord(wordId);
            if (shouldModify(currentEmbedding)) {
                underlyingExecutionEngine.processModification(currentEmbedding);
            }
            numChildrenEvaluated++;
            process(currentEmbedding);
            currentEmbedding.removeWord(currentEmbedding.getLastWord());
        }
        //if contraction
        else {
            currentEmbedding.removeWord(wordId);
            if (shouldModify(currentEmbedding)) {
                underlyingExecutionEngine.processModification(currentEmbedding);
            }
            numChildrenEvaluated++;
            process(currentEmbedding);
            currentEmbedding.addWord(wordId);
        }

    }

    private int modificationsSize(int[] modifications) {
        int i = 0, size = 0;
        while (i < modifications.length) {
            if (modifications[i] == -1) // all words
                size += currentEmbedding.getTotalNumWords();
            else if (modifications[i] < 0)
                size += (-1) * modifications[i];
            else
                size++;
            i++;
        }
        return size;
    }

    private int nextModification(int[] modifications, int upperBound) {
        int rdIdx = r.nextInt(upperBound);
        int i = 0, j = 0;
        while (i < modifications.length) {
            if (modifications[i] == -1) {
                if (j + currentEmbedding.getTotalNumWords() > rdIdx)
                    return rdIdx - j;
                else
                    j += currentEmbedding.getTotalNumWords();
            } else if (modifications[i] < 0) {
                if (j + (-1 * modifications[i]) > rdIdx)
                    for (int k = 0; k < (-1 * modifications[i]); k++) {
                        int nextRd = r.nextInt (getInitialNumWords());
                        if (j + k == rdIdx) return nextRd;
                    }
            } else {
                if (j + 1 > rdIdx)
                    return modifications[i];
                else
                    j++;
            }
            i++;
        }

        throw new RuntimeException ("Upper bound is greater than modifications");
    }

    @Override
    public void filter(E existingEmbedding, IntCollection modificationPoints) {

        //int previousDegree = modificationPoints.size();
        int[] modificationsArray = modificationPoints.toIntArray();
        int previousDegree = modificationsSize (modificationsArray);
        if (previousDegree == 0) {
            existingEmbedding.reset();
            return;
        }

        //int rdIdx = r.nextInt(previousDegree);
        //int nextModification = modificationPoints.toIntArray()[rdIdx];
        int nextModification = nextModification (modificationsArray, previousDegree);
        assert nextModification >= 0;
        int degree = 1;

        //if expansion
        if (!currentEmbedding.existWord(nextModification)) {
            currentEmbedding.addWord(nextModification);
            IntCollection modPoints = getPossibleModifications(currentEmbedding);
            //degree = modPoints.size();
            degree = modificationsSize (modPoints.toIntArray());
            currentEmbedding.removeWord(nextModification);
        }
        //if contraction
        else {
            currentEmbedding.removeWord(nextModification);
            IntCollection modPoints = getPossibleModifications(currentEmbedding);
            //degree = modPoints.size();
            degree = modificationsSize (modPoints.toIntArray());
            currentEmbedding.addWord(nextModification);
        }


        double accept = Math.min(1, (double) previousDegree/degree);

        modificationPoints.clear();

        if (r.nextDouble() <= accept)
            modificationPoints.add(nextModification);

    }

    @Override
    public boolean filter(E existingEmbedding, int newWord) {
        return true;
    }

    @Override
    protected IntCollection getPossibleModifications(E embedding) {
        IntCollection possibleModifications = getPossibleExtensions(embedding);
        possibleModifications.addAll(getPossibleContractions(embedding));
        return possibleModifications;
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
}


