package com.ido.robin.sstable;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * @author Ido
 * @date 2019/1/1 10:44
 */
@Slf4j
public class SparseIndexMem implements Index {
    private List<SegmentFile> segmentFiles;
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
    private String name;
    private IndexFile indexFile;


    public SparseIndexMem(List<SegmentFile> segmentFiles) {
        this("sparse-index", segmentFiles);

    }

    public SparseIndexMem(String name, List<SegmentFile> segmentFiles) {
        this.name = name;
        try {
            writeLock.lock();
            List<SegmentHeader> headers = segmentFiles.stream().map(i -> i.getHeader()).collect(Collectors.toList());
            IndexFile indexFile = new IndexFile();
            indexFile.setSegmentHeaders(headers);
            this.indexFile = indexFile;
            this.segmentFiles = segmentFiles;
        } catch (Throwable ex) {
            throw ex;
        } finally {
            writeLock.unlock();
        }


    }


    private static class KeySegmentFileData {
        private String keyEnd;
        private String keyStart;
        private String fileName;
    }

    @Override
    public Block search(String key) {
        try {
            readLock.lock();
            for (SegmentHeader header : indexFile.getSegmentHeaders()) {
                if (header.isKeyBetweenFile(key)) {
                    Optional<SegmentFile> sg = this.segmentFiles.stream().filter(s -> s.getOriginalFileName().equals(header.segmentFileName)).findFirst();
                    log.debug("key {} locate in file {} ", key, sg.get().getOriginalFileName());
                    return sg.map(segmentFile -> segmentFile.get(key)).orElse(null);
                }
            }

            KeySegmentFileData lowerKeyFile = new KeySegmentFileData();
            lowerKeyFile.keyStart = indexFile.getSegmentHeaders().get(0).keyStart;
            lowerKeyFile.keyEnd = indexFile.getSegmentHeaders().get(0).keyEnd;
            lowerKeyFile.fileName = indexFile.getSegmentHeaders().get(0).segmentFileName;
            KeySegmentFileData higherKeyFile = new KeySegmentFileData();
            higherKeyFile.keyStart = indexFile.getSegmentHeaders().get(0).keyStart;
            higherKeyFile.keyEnd = indexFile.getSegmentHeaders().get(0).keyEnd;
            higherKeyFile.fileName = indexFile.getSegmentHeaders().get(0).segmentFileName;
            //帅选出当前key 最贴近 的segment file
            for (SegmentHeader header : indexFile.getSegmentHeaders()) {
                if(header.fileLen == 0){
                    continue;
                }
                if (header.keyEnd.compareTo(key) <= 0 && header.keyEnd.compareTo(lowerKeyFile.keyEnd) >= 0) {
                    lowerKeyFile.fileName = header.segmentFileName;
                    lowerKeyFile.keyStart = header.keyStart;
                    lowerKeyFile.keyEnd = header.keyEnd;
                }
                if (header.keyStart.compareTo(key) >= 0 && header.keyStart.compareTo(higherKeyFile.keyStart) <= 0) {
                    higherKeyFile.fileName = header.segmentFileName;
                    higherKeyFile.keyStart = header.keyStart;
                    higherKeyFile.keyEnd = header.keyEnd;
                }

            }

            log.info("target file {}", Arrays.asList(lowerKeyFile.fileName, higherKeyFile.fileName));
            for (String f : Arrays.asList(lowerKeyFile.fileName, higherKeyFile.fileName)) {
                Optional<SegmentFile> sg = this.segmentFiles.stream().filter(s -> s.getOriginalFileName().equals(f)).findFirst();
                if (!sg.isPresent()) continue;
                if (sg.get().keyIsBetweenIn(key)) {
                    return sg.get().get(key);
                }

            }
        } finally {
            readLock.unlock();
        }
        return null;
    }


    @Override
    public void onChange(List<SegmentFile> segmentFiles, List<String> changedSgFiles) {

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SparseIndexMem that = (SparseIndexMem) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "SparseIndexMem{" +
                "name='" + name + '\'' +
                '}';
    }
}
