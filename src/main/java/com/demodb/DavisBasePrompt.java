package com.demodb;

import java.util.Scanner;

import com.demodb.query.handler.QueryHandler;
import com.demodb.util.ConsoleWriter;
import com.demodb.util.Constants;

/**
 * @author Chris Irwin Davis
 * @version 1.0 <b>
 *          <p>
 * 			This is an example of how to create an interactive prompt
 *          </p>
 *          <p>
 * 			There is also some guidance to get started wiht read/write of binary
 *          data files using RandomAccessFile class
 *          </p>
 *          </b>
 *
 */
public class DavisBasePrompt {
	static boolean isExit = false;
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	public static void main(String[] args) {
		DavisBaseInitiallizer.init();
		splashScreen();
		String userCommand = "";
		while (!isExit) {
			ConsoleWriter.write(Constants.PROMPT);
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			isExit = QueryHandler.handle(userCommand);
		}
		ConsoleWriter.write("Exiting...");
	}

	public static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + QueryHandler.getVersion());
		System.out.println(QueryHandler.getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}
}