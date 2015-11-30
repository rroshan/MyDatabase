import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class Client {
	public static void main(String[] args) {

		MyDatabase myDb = new MyDatabase();
		QueryProcessor qp = new QueryProcessor(myDb);

		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
		int message = 1;

		String directory = null;

		System.out.println("Welcome to Pharma Trials Database");
		System.out.print("Enter directory where database is located or you want to import to: ");
		try {
			directory = bufferedReader.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		File dir = new File(directory);

		if(!dir.exists()) {
			dir.mkdir();
		}

		qp.setDbLocation(dir.getAbsolutePath());

		if(dir.isDirectory()) {
			String command;

			while(true) {
				System.out.println();
				System.out.print("prompt> ");
				try {
					command = bufferedReader.readLine();
					message = qp.processQuery(command);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				if(message == -1) {
					System.out.println("Bye");
					System.exit(0);
				}
			}
		}
	}
}
