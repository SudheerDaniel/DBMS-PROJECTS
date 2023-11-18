import com.sun.xml.internal.ws.util.ServiceFinder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Scanner;

public class DBMSProj1 {

	public static class Actors{
		public String actorName;
		public String actorNumber;
	}
	public static class Movies{
		public String movieName;
		public String movieNumber;
	}
	public static class Play{
		public String actorNumber;
		public String movieNumber;
		public int payment;
	}
	public static void main(String[] args) throws IOException {
		List<Actors> actorDetails = new ArrayList<>();
		List<Movies> movieDetails = new ArrayList<>();
		List<Play> playDetails = new ArrayList<>();
		Scanner readScanner = new Scanner(new File("ACTORS.txt"));
		List<String> result1  = new ArrayList<>();
		String[] actorHeads = readScanner.nextLine().trim().split(", ");
		while(readScanner.hasNextLine()) {
			String[] actorDet = readScanner.nextLine().trim().split(", ");
			Actors actor = new Actors();
			actor.actorName = actorDet[0];
			actor.actorNumber = actorDet[1];
			actorDetails.add(actor);
		}
		readScanner.close();


		readScanner = new Scanner(new File("Movies.txt"));
		String[] movieHeads = readScanner.nextLine().trim().split(", ");
		while(readScanner.hasNextLine()) {
			String[] movieDet = readScanner.nextLine().trim().split(", ");
			Movies movie = new Movies();
			movie.movieName = movieDet[0];
			movie.movieNumber = movieDet[1];
			movieDetails.add(movie);
		}
		readScanner.close();
		readScanner = new Scanner(new File("Play.txt"));
		String[] playHeads = readScanner.nextLine().trim().split(", ");
		while(readScanner.hasNextLine()) {
			String[] playDet = readScanner.nextLine().trim().split(", ");
			Play play = new Play();
			play.actorNumber = playDet[0];
			play.movieNumber = playDet[1];
			play.payment = Integer.parseInt(playDet[2]);
			playDetails.add(play);
		}
		HashMap<String, String[]> tableMap = new HashMap<>();
		tableMap.put("Play", playHeads);
		tableMap.put("ACTORS", actorHeads);
		tableMap.put("MOVIES", movieHeads);
		readScanner.close();
		readScanner = new Scanner(new File("RAqueries.txt"));
		while(readScanner.hasNextLine()) {
			String query = readScanner.nextLine().trim();
			List<String[]> result = new LinkedList<>();
			if(query.startsWith("SELE_")) {
				result = selectTables(query, actorDetails, movieDetails, playDetails, actorHeads, movieHeads, playHeads);
				for(int i =0;i<result.size();i++) {
					System.out.println(Arrays.toString(result.get(i)));
				}
				writeData(result, false,query);
			}else if(query.startsWith("PROJ_") && !query.contains("*")) {
				//Only projection operation
				result =  new LinkedList<>();
				String qryTemp = query;
				String tableName = query.substring(query.indexOf("}")+1).trim();
				String[] columns = query.substring(query.indexOf("{")+1, query.indexOf("}")).split(", ");
				String[] data = tableMap.get(tableName);


				List<Movies> tabDetails = new ArrayList<>();
				List<String> cols = Arrays.asList(columns);

				if("ACTORS".equals(tableName)) {
					for(Actors actor : actorDetails) {
						String str ="";
						if(cols.contains("ANAME"))
							str+=actor.actorName+",";
						if(cols.contains("ANO"))
							str = str+actor.actorNumber+",";
						result.add(str.split(","));

					}
				}else if("MOVIES".equals(tableName)) {
					for(Movies movie : movieDetails) {
						String str ="";
						if(cols.contains("MNAME"))
							str+=movie.movieName+",";
						if(cols.contains("MNO"))
							str = str+movie.movieNumber+",";
						result.add(str.split(","));
					}

				}else if("Play".equals(tableName)) {
					for(Play play : playDetails) {
						String str ="";
						//ANO, MNO, Payment
						if(cols.contains("ANO"))
							str+=play.actorNumber+",";
						if(cols.contains("MNO"))
							str = str+","+play.movieNumber+",";
						if(cols.contains("Payment"))
							str = str+","+play.payment+",";
						result.add(str.split(","));

					}
				}
				writeData(result, true,query);

			}else if (query.startsWith("PROJ_") && query.contains("*")) {
				//Projection on Join other operator
				String columns[] = query.substring(query.indexOf("{")+1, query.indexOf("}")).split(", ");
				String subquery = query.substring(query.indexOf("(")+1, query.indexOf(")"));
				List<String[]> subresult = joinTables(subquery, actorDetails, movieDetails, playDetails, actorHeads, movieHeads, playHeads);
				List<Integer> colNum = new ArrayList<>();
				result = new LinkedList<String[]>();
				for(int i =0;i<subresult.get(0).length;i++)
					for(int j =0;j<columns.length;j++)
						if(columns[j].equals(subresult.get(0)[i]))
							colNum.add(i);
				for(String[] sr : subresult) {
					String s ="";
					for(int i : colNum) {
						System.out.println(sr[i]);
						s+=sr[i]+",";
					}
					result.add(s.split(","));
				}

				writeData(result, true,query);

			}
			else if (query.startsWith("(") && query.contains("U")) {
				// Projection on any other operator
				System.out.println("In Union");
				String query1 = query.split("U")[0].trim();
				String query2 = query.split("U")[1].trim();

				String[] columns1 = query1.substring(query1.indexOf("{") + 1, query1.indexOf("}")).split(", ");
				String[] columns2 = query2.substring(query2.indexOf("{") + 1, query2.indexOf("}")).split(", ");

				// Process the first subquery
				List<String[]> subresult1 = selectTables(query1.substring(query1.indexOf(" ")+2, query1.indexOf(")")+1), actorDetails, movieDetails, playDetails, actorHeads, movieHeads, playHeads);

				boolean firstRow1 = true;

				List<String> result1_f = new ArrayList<>();

				for (String[] row : subresult1) {
//					if (firstRow1) {
//						firstRow1 = false;
//						continue; // Skip the header row
//					}
					System.out.println(row[0]);
					result1_f.add(row[0]);
				}

				// Process the second subquery
				List<String[]> subresult2 = selectTables(query2.substring(query2.indexOf(" ")+2, query2.indexOf(")")+1), actorDetails, movieDetails, playDetails, actorHeads, movieHeads, playHeads);

				boolean firstRow2 = true;

				List<String> result2 = new ArrayList<>();

				for (String[] row : subresult2) {
					if (firstRow2) {
						firstRow2 = false;
						continue; // Skip the header row
					}
					System.out.println(row[0]);
					result2.add(row[0]);
				}

				// Combine the two lists
				result1_f.addAll(result2);

				// Write the result to the output file
				writeData2(result1_f, true, query);
			}

			else if (query.startsWith("(") && query.contains("-")) {
				// Projection on any other operator
				String query1 = query.split("-")[0].trim();
				String query2 = query.split("-")[1].trim();

				// Process the first subquery
				List<String[]> subresult1 = selectTables(query1.substring(query1.indexOf(" ")+2, query1.indexOf(")")+1), actorDetails, movieDetails, playDetails, actorHeads, movieHeads, playHeads);

				// Process the second subquery
				List<String[]> subresult2 = selectTables(query2.substring(query2.indexOf(" ")+2, query2.indexOf(")")+1), actorDetails, movieDetails, playDetails, actorHeads, movieHeads, playHeads);

				// Create a new result list to store the set difference
				List<String[]> resultSetDifference = new ArrayList<>();
				for (String[] row1 : subresult1) {
					String uniqueValue1 = row1[0]; // Assuming the unique identifier is in the first column
					boolean foundInSubresult2 = false;

					for (String[] row2 : subresult2) {
						String uniqueValue2 = row2[0]; // Assuming the unique identifier is in the first column

						if (uniqueValue1.equals(uniqueValue2)) {
							foundInSubresult2 = true;
							break;
						}
					}

					if (!foundInSubresult2) {
						resultSetDifference.add(row1);
					}
				}

				// Append the column name to the output
				if (!resultSetDifference.isEmpty()) {
					String[] header = {actorHeads[1]}; // Add the column name
					resultSetDifference.add(0, header);
				}

				LinkedList<String[]> final_list = new LinkedList<>();
				for (String[] i : resultSetDifference) {
					final_list.add(new String[]{i[0]});
				}

				// Write the result to the output file
				writeData(final_list, true, query);
			}



		}
	}

	public static List<String[]> selectTables(String query, List<Actors> actorDetails, List<Movies> movieDetails,List<Play> playDetails, String[] actorHeads, String[] movieHeads, String[] playHeads){

		List<String[]> result = new LinkedList<>();
		String qryTemp = query;
		String condition = qryTemp.substring(qryTemp.indexOf("{")+1, qryTemp.indexOf("}"));
		String column,value="";
		if(condition.contains("=")) {
			column = qryTemp.substring(qryTemp.indexOf("{") + 1, qryTemp.indexOf("}")).split("=")[0];
			value = qryTemp.substring(qryTemp.indexOf("{") + 1, qryTemp.indexOf("}")).split("=")[1];
		}
		else{
			column = qryTemp.substring(qryTemp.indexOf("{") + 1, qryTemp.indexOf("}")).split(" ")[0];
			value = qryTemp.substring(qryTemp.indexOf("{") + 1, qryTemp.indexOf("}")).split(" ")[2];
		}
		//System.out.println(value);
		String line = query;
		int startIndex = line.indexOf("(")+1;
		int endIndex = line.indexOf(")");
		String tableName = line.substring(startIndex, endIndex).trim();
		String operator = condition.substring(condition.indexOf(" ")+1).split(" ")[0];

		if(tableName.equals("ACTORS")) {
			result.add(actorHeads);
			if(column.equals(actorHeads[0])) {
				for(int i=0;i<actorDetails.size();i++) {
					if(actorDetails.get(i).actorName.equals(value))
						result.add(new String[] {actorDetails.get(i).actorName, actorDetails.get(i).actorNumber});
				}
			}else {
				for(int i=0;i<actorDetails.size();i++) {
					if(actorDetails.get(i).actorNumber.equals(value))
						result.add(new String[] {actorDetails.get(i).actorName, actorDetails.get(i).actorNumber});
				}

			}

		}else if(tableName.equals("MOVIES")) {
			result.add(movieHeads);
			if(column.equals(movieHeads[0])) {
				for(int i=0;i<actorDetails.size();i++) {
					if(movieDetails.get(i).movieName.equals(value))
						result.add(new String[] {movieDetails.get(i).movieName, movieDetails.get(i).movieNumber});
				}
			}else {
				for(int i=0;i<actorDetails.size();i++) {
					if(movieDetails.get(i).movieNumber.equals(value))
						result.add(new String[] {movieDetails.get(i).movieName, movieDetails.get(i).movieNumber});
				}

			}

		}else if(tableName.equals("Play")) {
			result.add(playHeads);
			if(column.equals(playHeads[0])) {
				for(int i=0;i<playDetails.size();i++) {
					if(playDetails.get(i).actorNumber.equals(value))
						result.add(new String[] {playDetails.get(i).actorNumber, playDetails.get(i).movieNumber, playDetails.get(i).payment+""});
				}
			}else if(column.equals(playHeads[1])){
				for(int i=0;i<playDetails.size();i++) {
					if(playDetails.get(i).movieNumber.equals(value))
						result.add(new String[] {playDetails.get(i).actorNumber, playDetails.get(i).movieNumber, playDetails.get(i).payment+""});
				}

			}else {
				for(int i = 0;i<playDetails.size();i++) {
					if(operator.equals(">")) {
						if(playDetails.get(i).payment > Integer.parseInt(value))
							result.add(new String[] {playDetails.get(i).actorNumber, playDetails.get(i).movieNumber, playDetails.get(i).payment+""});
					}
					if(operator.equals("<")) {
						if(playDetails.get(i).payment < Integer.parseInt(value))
							result.add(new String[] {playDetails.get(i).actorNumber, playDetails.get(i).movieNumber, playDetails.get(i).payment+""});
					}
					if(operator.equals("=")) {
						if(playDetails.get(i).payment == Integer.parseInt(value))
							result.add(new String[] {playDetails.get(i).actorNumber, playDetails.get(i).movieNumber, playDetails.get(i).payment+""});
					}
				}
			}

		}

		return result;

	}


	public static List<String[]> joinTables(String query, List<Actors> actorDetails, List<Movies> movieDetails,List<Play> playDetails, String[] actorHeads, String[] movieHeads, String[] playHeads){
		List<String[]> result = new LinkedList<String[]>();
		String table1 = query.split("\\* ")[0].trim();
		String table2 = query.split("\\* ")[1].trim();
		String[] tab1Details = null;
		String[] tab2Details = null;
		//System.out.println(table1+"----"+table2);
		if(table1.equals("ACTORS")) {
			tab1Details = actorHeads;
			tab2Details = playHeads;
		}else if(table1.equals("MOVIES")) {
			tab1Details = actorHeads;
			tab2Details = playHeads;
		}else if(table1.equals("Play")) {
			tab1Details = playHeads;
			if(table2.equals("MOVIES"))
				tab2Details = movieHeads;
			if(table1.equals("ACTORS"))
				tab2Details = actorHeads;
		}

		if(table1.equals("ACTORS") || table2.equals("ACTORS")) {
			result.add(new String[] {"ANAME","ANO","MNO","Payment"});
			for(Play pl : playDetails) {
				for(Actors actor : actorDetails) {
					if(actor.actorNumber.equals(pl.actorNumber)) {
						result.add(new String[] {actor.actorName,pl.actorNumber,pl.movieNumber,pl.payment+""});
						break;
					}
				}
			}
		}else if(table1.equals("MOVIES") || table2.equals("MOVIES")) {
			result.add(new String[] {"MNAME","ANO","MNO","Payment"});
			for(Play pl : playDetails) {
				for(Movies movie : movieDetails) {
					if(movie.movieNumber.equals(pl.movieNumber)) {
						result.add(new String[] {movie.movieName,pl.actorNumber,pl.movieNumber,pl.payment+""});
						break;
					}
				}
			}
		}
		return result;
	}

	public static void writeData(List<String[]> result, boolean isAppend, String query) throws IOException {
		FileWriter write = new FileWriter(new File("RAoutput.csv"), isAppend);
		write.write(query+"\n");
		for(int i =0;i<result.size();i++) {
			for(int j = 0;j<result.get(i).length;j++) {
				write.write(result.get(i)[j]+",");
			}
			write.write("\n");
		}
		write.close();
	}

	public static void writeData2(List<String> result, boolean isAppend, String query) throws IOException {
		FileWriter write = new FileWriter(new File("RAoutput.csv"), isAppend);
		write.write(query+"\n");
		for(int i =0;i<result.size();i++) {
			write.write(result.get(i)+"\n");
		}
		write.close();
	}


}