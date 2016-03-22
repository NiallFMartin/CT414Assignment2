// CT414 Assignment 2 - Map Reduce Cluster Simulation
// Niall Martin - 12301341
// Shane O'Rourke - 12361351

package mapReduce;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MapReduce {

	public static void main(String[] args) throws IOException {

		long startTime = System.nanoTime();

		MapReduce mp = new MapReduce();
		////////////
		// INPUT:
		///////////

			
		//import command line parameters	
		int numThreads = Integer.parseInt(args[0]);
		
		String s1 = args[1];
		File file1 = new File(s1);
		String s2 = args[2];
		File file2 = new File(s2);
		String s3 = args[3];
		File file3 = new File(s3);

		String file1Contents = mp.readWordsFromFile(file1);
		String file2Contents = mp.readWordsFromFile(file2);

		String file3Contents = mp.readWordsFromFile(file3);

		// Set number of threads to be executed to number specified in command line parameter.
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);

		Map<String, String> input = new HashMap<String, String>();

		input.put(file1.getName(), file1Contents);
		input.put(file2.getName(), file2Contents);
		input.put(file3.getName(), file3Contents);

		// APPROACH #3: Distributed MapReduce
		final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

		/*******************************************************************************************
		 * MAP:
		 */
		final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
		final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
			@Override
			public synchronized void mapDone(String file, List<MappedItem> results) {
				mappedItems.addAll(results);
			}
		};

		List<Thread> mapCluster = new ArrayList<Thread>(input.size());

		Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
		while (inputIter.hasNext()) {
			Map.Entry<String, String> entry = inputIter.next();
			final String file = entry.getKey();
			final String contents = entry.getValue();

			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					map(file, contents, mapCallback);
				}
			});
			// Add Thread t to List of threads.
			mapCluster.add(t);
		}

		// Iterate through the list and execute each thread.
		for (Thread t : mapCluster) {
			executor.execute(t);
		}

		// When finished shut down all the threads.
		executor.shutdown();
		// Wait until executor is finished and shutdown.
		while (!executor.isTerminated())
			;

		// GROUP:
		Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

		Iterator<MappedItem> mappedIter = mappedItems.iterator();
		while (mappedIter.hasNext()) {
			MappedItem item = mappedIter.next();
			String word = item.getWord();
			String file = item.getFile();
			List<String> list = groupedItems.get(word);
			if (list == null) {
				list = new LinkedList<String>();
				groupedItems.put(word, list);
			}
			list.add(file);
		}

		/*******************************************************************************************
		 * REDUCE:
		 */
		final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
			@Override
			public synchronized void reduceDone(String k, Map<String, Integer> v) {
				output.put(k, v);
			}
		};

		// Re-initialise executor object.
		executor = Executors.newFixedThreadPool(5);

		List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

		Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
		while (groupedIter.hasNext()) {
			Map.Entry<String, List<String>> entry = groupedIter.next();
			final String word = entry.getKey();
			final List<String> list = entry.getValue();

			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					reduce(word, list, reduceCallback);
				}
			});
			// Add Thread t to List of threads.
			reduceCluster.add(t);
		}

		// Iterate through the list and execute each thread.
		for (Thread t : reduceCluster) {
			executor.execute(t);
		}

		// When finished shut down all the threads.
		executor.shutdown();
		// Wait until executor is finished and shutdown.
		while (!executor.isTerminated())
			;

		System.out.println(output);
		long endTime = System.nanoTime();
		double seconds = ((double) (endTime - startTime) / 1000000000);

		System.out.println("Time taken to execute: \n" + seconds + " SECONDS");
	}

	/************************************************************************************************
	 * End of main
	 */

	public static void map(String file, String contents, List<MappedItem> mappedItems) {
		String[] words = contents.trim().split("\\s+");
		for (String word : words) {
			mappedItems.add(new MappedItem(word, file));
		}
	}

	public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for (String file : list) {
			Integer occurrences = reducedList.get(file);
			if (occurrences == null) {
				reducedList.put(file, 1);
			} else {
				reducedList.put(file, occurrences.intValue() + 1);
			}
		}
		output.put(word, reducedList);
	}

	public static interface MapCallback<E, V> {

		public void mapDone(E key, List<V> values);
	}

	public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
		String[] words = contents.trim().split("\\s+");
		List<MappedItem> results = new ArrayList<MappedItem>(words.length);
		for (String word : words) {
			results.add(new MappedItem(word, file));
		}
		callback.mapDone(file, results);
	}

	public static interface ReduceCallback<E, K, V> {

		public void reduceDone(E e, Map<K, V> results);
	}

	public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

		Map<String, Integer> reducedList = new HashMap<String, Integer>();
		for (String file : list) {
			Integer occurrences = reducedList.get(file);
			if (occurrences == null) {
				reducedList.put(file, 1);
			} else {
				reducedList.put(file, occurrences.intValue() + 1);
			}
		}
		callback.reduceDone(word, reducedList);
	}

	// Returns a string of every word in a file.
	public String readWordsFromFile(File file) throws IOException {

		StringBuilder sb = new StringBuilder();
		Scanner scan1 = null;
		try {
			scan1 = new Scanner(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		while (scan1.hasNextLine()) {
			Scanner scan2 = new Scanner(scan1.nextLine());
			while (scan2.hasNext()) {
				String s = scan2.next();
				sb.append(s + " ");
			}
		}
		return sb.toString();
	}

	private static class MappedItem {

		private final String word;
		private final String file;

		public MappedItem(String word, String file) {
			this.word = word;
			this.file = file;
		}

		public String getWord() {
			return word;
		}

		public String getFile() {
			return file;
		}

		@Override
		public String toString() {
			return "[\"" + word + "\",\"" + file + "\"]";
		}
	}
}
