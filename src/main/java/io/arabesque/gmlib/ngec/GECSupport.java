package io.arabesque.gmlib.ngec;

import io.arabesque.embedding.Embedding;
import io.arabesque.utils.ClearSetConsumer;
import io.arabesque.utils.IntWriterConsumer;
import io.arabesque.utils.collection.IntArrayList;
import io.arabesque.utils.collection.IntCollectionAddConsumer;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.set.IntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;
import org.apache.hadoop.io.Writable;

import java.util.Arrays;
import java.io.*;

public class GECSupport implements Writable, Externalizable {
    private static final ThreadLocal<ClearSetConsumer> clearSetConsumer =
            new ThreadLocal<ClearSetConsumer>() {
                @Override
                protected ClearSetConsumer initialValue() {
                    return new ClearSetConsumer();
                }
            };

    private HashIntSet[] gecSets;
    private int support;
    private int currentSupport;
    private IntWriterConsumer intWriterConsumer;
    private IntCollectionAddConsumer intAdderConsumer;
    private Embedding embedding;
    private boolean setFromEmbedding;

    public static final int INC_GECSETS = 50;

    public GECSupport() {
        this.currentSupport = 0;
        ensureCanStoreNGECs(INC_GECSETS);
        setFromEmbedding = false;
    }

    public GECSupport(int support) {
        this();
        this.support = support;
    }

    public void setFromEmbedding(Embedding embedding) {
        setFromEmbedding = true;
        this.embedding = embedding;
    }

    public int getSupport() {
        return support;
    }

    public int getCurrentSupport() {
        return currentSupport;
    }

    public void clear() {
        clearGECs();
    }

    private HashIntSet getGECSet(int i) {
        //increase gecSets slot by const
        if (i>=gecSets.length)
            ensureCanStoreNGECs(i+INC_GECSETS);

        HashIntSet gec = gecSets[i];
        if (gec == null) {
            gec = gecSets[i] = HashIntSets.newMutableSet();
        }

        return gec;
    }

    public void ensureCanStoreNGECs(int size) {
        if (gecSets == null) {
            this.gecSets = new HashIntSet[size];
        } else if (gecSets.length < size) {
            gecSets = Arrays.copyOf(gecSets, size);
        }
    }


    private void clearGECs() {
        if (gecSets != null) {
            for (int i = 0; i < gecSets.length; ++i) {
                IntSet gec = gecSets[i];
                if (gec != null) {
                    gec.clear();
                }
            }
        }
    }

    public void writeExternal(ObjectOutput objOutput) throws IOException {
       write (objOutput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeInt(support);
        dataOutput.writeInt(currentSupport);

        if (intWriterConsumer == null) {
            intWriterConsumer = new IntWriterConsumer();
        }
        intWriterConsumer.setDataOutput(dataOutput);
        for (int i = 0; i < currentSupport; ++i) {
            dataOutput.writeInt(gecSets[i].size());
            gecSets[i].forEach(intWriterConsumer);
        }
    }

    @Override
    public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
       readFields (objInput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.clear();

        support = dataInput.readInt();
        currentSupport = dataInput.readInt();

        for (int i = 0; i < currentSupport; ++i) {
            int domainSize = dataInput.readInt();
            HashIntSet domainSet = getGECSet(i);
            for (int j = 0; j < domainSize; ++j) {
                domainSet.add(dataInput.readInt());
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GspanPatternSupportAggregation{" +
                ", support=" + support +
                ", currentSupport=" + currentSupport +
                ", numberOfGECs=" + currentSupport);

        if (gecSets != null) {
            for (int i = 0; i < currentSupport; i++) {
                HashIntSet gecSet = gecSets[i];
                if (gecSet == null) {
                    continue;
                }
                sb.append(",gec[" + i + "]=" + gecSets[i]);
            }
        }
        sb.append('}');

        return sb.toString();
    }

    public String toStringResume() {
        StringBuilder sb = new StringBuilder();
        sb.append("GspanPatternSupportAggregation{" +
                ", support=" + support +
                ", currentSupport=" + currentSupport +
                ", numberOfGECs=" + currentSupport);

        if (gecSets != null) {
            for (int i = 0; i < currentSupport; i++) {
                HashIntSet gecSet = gecSets[i];
                if (gecSet == null) {
                    continue;
                }
                sb.append(",domain[" + i + "]=" + gecSet.size());
            }
        }
        sb.append('}');

        return sb.toString();
    }

    private void addAll(IntCollection destination, IntCollection source) {
        if (intAdderConsumer == null) {
            intAdderConsumer = new IntCollectionAddConsumer();
        }
        intAdderConsumer.setCollection(destination);
        source.forEach(intAdderConsumer);
    }

    private IntArrayList getGECSOfSet(IntCollection set) {
        IntArrayList gecsIdxs = new IntArrayList();
        for (int i = 0; i < currentSupport; ++i) {
            HashIntSet gec = getGECSet(i);
            for (int j : set) {
                if (gec.contains(j)) {
                    gecsIdxs.add(i);
                }
            }
        }
        return gecsIdxs;
    }

    private void swapGECSets(int i, int j) {
        gecSets[i] = gecSets[j];
    }

    private void mergeGECs(IntCollection list) {
        if (list.size()==0) return;

        boolean isFirstGEC = true;
        HashIntSet firstGEC = null;
        int firstGECIdx = 0;

        int map[] = new int[currentSupport]; //with zeros

        for (int i : list) {
            if (isFirstGEC) {
                isFirstGEC = false;
                firstGECIdx = i;
                firstGEC = getGECSet(i);
            }
            else {
                int idx = i;
                if (map[i]>firstGECIdx)
                    idx = map[i];
                HashIntSet gec = getGECSet(idx);
                addAll(firstGEC, gec);
                gec.clear();

                //reducing the gecs
                currentSupport--;
                swapGECSets(idx, currentSupport);
                map[currentSupport] = idx;
            }
        }
    }

    public void aggregate(final HashIntSet[] gecs, int currSupp) {
        for (int i = 0; i < currSupp; ++i) {
            aggregate(gecs[i]);
        }
    }

    private void embeddingAggregate(Embedding embedding) {
        IntArrayList edges = embedding.getEdges();
        aggregate(edges);
    }

    private void aggregate(IntCollection set)
    {
        IntArrayList eqGECs = getGECSOfSet(set);
        if (set.size()==0) {
            HashIntSet gec = getGECSet(currentSupport); //new gec
            gec.addAll(set);
            currentSupport++;
            return;
        }
        else {
            HashIntSet gec = getGECSet(eqGECs.get(0)); //get the first
            gec.addAll(set);
            mergeGECs(eqGECs);
        }
    }

    public void aggregate(GECSupport other) {
        if (this == other)
            return;

        if (setFromEmbedding) {
            embeddingAggregate(embedding);
            return;
        }

        aggregate(other.gecSets, other.getCurrentSupport());
    }

}
