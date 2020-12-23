package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);

        Exchanger<List<Pair<String, Integer>>> exchanger = new Exchanger<>();
        Phaser phaser = new Phaser(CHUNK_SIZE);

        Thread writerThread = new Thread(new FileWriter(resultFileName, exchanger));
        writerThread.start();

        ExecutorService executorService = Executors.newFixedThreadPool(CHUNK_SIZE);

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
                List<Pair<String, Integer>> resultPairList = new ArrayList<>();
                List<String> lineList = new ArrayList<>();

                while (lineList.size() < CHUNK_SIZE && scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    lineList.add(line);
                }

                if (lineList.size() >= CHUNK_SIZE) {
                    phaser.arriveAndDeregister();
                }

                for (int i = 0; i < lineList.size(); i++) {

                    int counter = i;
                    executorService.submit(() -> {
                        resultPairList.set(counter, new LineCounterProcessor().process(lineList.get(counter)));
                    });

                    phaser.arrive();
                }

                phaser.arriveAndAwaitAdvance();
                exchanger.exchange(resultPairList);
            }

        } catch (IOException | InterruptedException exception) {
            logger.error("", exception);
        }

        writerThread.interrupt();
        executorService.shutdown();

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}



