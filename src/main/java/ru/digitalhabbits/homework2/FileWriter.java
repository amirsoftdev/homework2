package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Exchanger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter implements Runnable {

    private static final Logger logger = getLogger(FileWriter.class);
    private final String resultFile;
    private final Exchanger<List<Pair<String, Integer>>> exchanger;

    public FileWriter(String resultFile, Exchanger<List<Pair<String, Integer>>> exchanger) {
        this.resultFile = resultFile;
        this.exchanger = exchanger;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());
        try {
            BufferedWriter writer = new BufferedWriter(new java.io.FileWriter(resultFile));

            while (!currentThread().isInterrupted()) {
                List<Pair<String, Integer>> pairList = exchanger.exchange(null);

                for (Pair<String, Integer> pair : pairList) {
                    writer.write(pair.getLeft() + " " + pair.getRight() + "\n");
                }
            }
            writer.flush();

            logger.info("Finish writer thread {}", currentThread().getName());
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}