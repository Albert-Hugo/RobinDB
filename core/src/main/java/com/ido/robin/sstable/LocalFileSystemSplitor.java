package com.ido.robin.sstable;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.ido.robin.sstable.SegmentFile.SEGMENT_FILE_SUFFIX;


/**
 * local file system splittor to split exceed segment file
 *
 * @author Ido
 * @date 2019/1/11 14:30
 */
@Slf4j
public class LocalFileSystemSplitor implements FileSplitor {
    /**
     * segment file 最大的block list 数
     */
    private int maxBlockListSize = 5000;

    public LocalFileSystemSplitor() {
    }

    public LocalFileSystemSplitor(int maxBlockListSize) {
        this.maxBlockListSize = maxBlockListSize;
    }

    @Override
    public List<SegmentFile> listToBeSplittedSgFile(String path) {
        try {
            return Files.list(Paths.get(path))
                    .filter(p -> {
                        return p.toFile().getName().endsWith(SEGMENT_FILE_SUFFIX);
                    })
                    .map(Path::toFile)

                    .map(f -> {
                        try {
                            return new Node(f.getAbsolutePath(), SegmentHeader.getHeaderInfo(f.getAbsolutePath()));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .filter(f -> {
                        return f.header.blockListSize > this.maxBlockListSize;
                    })
                    .map(f -> {
                        try {
                            return new SegmentFile(f.name);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return null;
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return new ArrayList<>(0);
    }

    @Override
    public List<String> split(SegmentFile r) {
        synchronized (r.getOriginalFileName().intern()) {
            int count = 0;
            List<String> resultFiles = new ArrayList<>();
            List<Block> blocks = r.getBlockList();
            log.info("segment file [{}]初始 block 数量为{}", r.getOriginalFileName(), blocks.size());
            if (!new File(r.getOriginalFileName()).delete()) {
                log.error("文件删除失败 :{}", r.getOriginalFileName());
                return Collections.EMPTY_LIST;
            }


            List<Block> boundle = new ArrayList<>();
            for (Block block : blocks) {
                boundle.add(block);
                if (boundle.size() == this.maxBlockListSize) {
                    count = boundle.size() + count;
                    // create a new segment file
                    bundleBlockToFIle(r.getPath(), resultFiles, boundle);
                    boundle.clear();
                }

            }

            if (!boundle.isEmpty()) {
                count = boundle.size() + count;
                bundleBlockToFIle(r.getPath(), resultFiles, boundle);
            }
            if (count != blocks.size()) {
                log.error("split file total size [{}] is not equal original block size [{}]! ", count, blocks.size());
            }


            return resultFiles;
        }
    }

    private static class Node {
        String name;
        SegmentHeader header;

        public Node(String name, SegmentHeader header) {
            this.name = name;
            this.header = header;
        }

        public String getName() {
            return name;
        }

        public Node setName(String name) {
            this.name = name;
            return this;
        }

        public SegmentHeader getHeader() {
            return header;
        }

        public Node setHeader(SegmentHeader header) {
            this.header = header;
            return this;
        }
    }

    /**
     * 将打包的block 生成新的 segment file
     *
     * @param path        数据存储路径
     * @param resultFiles 生成的新文件名
     * @param boundle     待转换的block list
     */
    private void bundleBlockToFIle(String path, List<String> resultFiles, List<Block> boundle) {
        String fName = UUID.randomUUID().toString().replace("-", "") + SEGMENT_FILE_SUFFIX;
        try (SegmentFile file = new SegmentFile(path + fName)) {
            file.addBlockList(boundle);
            file.flush();
            log.debug("splitted file {},key range {} ,{}  ", file.getOriginalFileName());
            log.info("新segment file [{}] , block 数量 [{}]", fName, boundle.size());
            resultFiles.add(file.getOriginalFileName());
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }


    }
}
