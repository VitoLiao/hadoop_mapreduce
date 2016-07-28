package com.utsc.RS.MR.ProgramRecUseCosine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProRecUseCosineMapper4 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// System.out.println("Map4 Starting");
		// input
		// C2CODE|DIRECTORS|CASTS|SPLIT_WORD|YEAR|GENRES|COUNTRIES|LANGUAGES|WRITERS|RATING|IMAGE|SEQID|NAME|TYPE
		// |ROOTCATCODES|ROOTCATNAMES|HDFLAG|LEAFCATCODES|LEAFCATNAMES
		// keyText.set(key.toString());
		// valueText.set(value);
		// context.write(keyText, valueText);
		// output
		// key
		// value
		// SEQIDa|C2CODEa|SEQIDb|C2CODEb|NAMEa|NAMEb|RATINGb|ROOTCATCODESb|ROOTCATNAMESb|SCORE|IMAGEb|COMMENTb|HDFLAGb|TYPE

		if (value.toString().length() > 0) {
			String[] str = value.toString().trim().split("###", -1);
			String[] bS = str[0].split("##", -1);
			String[] aS = str[1].split("\\|", -1)[0].split("##", -1);
			// System.out.println("bs= "+str[0]);
			// System.out.println("as= "+str[1].split("\\|",-1)[0]);
			// System.out.println("as size= "+ aS.length);
			// System.out.println("bs size= "+ bS.length);
			Comparator<Record> comparator = new Comparator<Record>() {
				@Override
				public int compare(Record r1, Record r2) {
					if (r1.iScore > r2.iScore) {
						return -1;
					} else if (r1.iScore == r2.iScore) {
						return 0;
					} else {
						return 1;
					}
				}
			};

			// display prgram number for each program
			// int pLen = 10;
			int pLen = Integer.valueOf(context.getConfiguration().get("pLen"));
			ProRecUseCosineMapper4 RCR = new ProRecUseCosineMapper4();
			boolean debugflag = false;
			if (debugflag) {
				System.out.println("in Error");
				for (int i = 0; i < aS.length; i++) {
					String[] aaSP = aS[i].trim().split("\\|", -1);
					for (int j = 0; j < bS.length; j++) {
						int coorelateScore = 1000;
						String[] bbSP = bS[j].trim().split("\\|", -1);
						if (aaSP[0].equals(bbSP[0])) {
							coorelateScore = 0;
						} else {
							keyText.set(aaSP[0].trim());
							valueText.set(String.valueOf(coorelateScore) + "|" + bbSP[0].trim());
							context.write(keyText, valueText);
						}
					}
				}
			} else {
				for (int i = 0; i < aS.length; i++) {
					ArrayList<Record> ScoreHDH = new ArrayList<Record>();
					ArrayList<Record> ScoreHDL = new ArrayList<Record>();
					// String aa = bS[1];
					// System.out.println("i= "+i);
					// aS[i]="02000001000000090000000000036294|閻忓繑鍨甸敓锟�?,闁活澀绲婚～瀣礈閿燂拷?,閻犳劑鍨荤划顢傜紓浣藉閿燂拷?,婵炲娲橀敓锟�?,缂佺虎浜滈悿鍓曢悘蹇斿灥閸斿綊寮▎鎺戜粶,濞达絾鎹侀敓锟�?,闁哄倷鍗抽敓锟�?,婵炲娲橀敓锟�?,闁汇垽娼ч敓锟�?,濞达絿濮撮敓锟�?,閻㈩垽鎷�,闁瑰瓨鍨抽敓锟�?,濞寸姵鐗旈敓锟�?,濞达絿濮抽敓锟�?,闁哥喎锕ら弲锕�2016|闁活澀绲婚～瀣礈椤倻绱樻惔鈩冪|闁兼槒椴搁弸鍎勯柣妤勵潐濠�鏇㈠疾閿燂拷?,濡炶鍓熼敓锟�?";
					String[] aaSP = bS[Integer.valueOf(aS[i])].trim().split("\\|", -1);
					// if (aaSP[0].equals("02000002000000012015070798000036")) {
					// System.out.println("xiao= " +
					// bS[Integer.valueOf(aS[i])]);
					for (int j = 0; j < bS.length; j++) {
						// System.out.println("aS[i] j"+aS[i]+" "+j);
						// System.out.println("as" +
						// bS[Integer.valueOf(aS[i])]);
						// System.out.println("bS[j].trim() " +
						// bS[j].trim());
						// System.out.println("in normal");
						double coorelateScore = 0;
						// bS[j]="02000001000000090000000000036294|闁活澀绲婚～瀣礈閿燂拷?,閻忓繑鍨甸敓锟�?,閻犳劑鍨荤划顢傜紓浣藉閿燂拷?,婵炲娲橀敓锟�?,缂佺虎浜滈悿鍓曢悘蹇斿灥閸斿綊寮▎鎺戜粶,濞达絾鎹侀敓锟�?,闁哄倷鍗抽敓锟�?,婵炲娲橀敓锟�?,闁汇垽娼ч敓锟�?,濞达絿濮撮敓锟�?,閻㈩垽鎷�,闁瑰瓨鍨抽敓锟�?,濞寸姵鐗旈敓锟�?,濞达絿濮抽敓锟�?,闁哥喎锕ら弲锕�2016|闁活澀绲婚～瀣礈椤倻绱樻惔鈩冪|闁兼槒椴搁弸鍎勯柣妤勵潐濠�鏇㈠疾閿燂拷?,濡炶鍓熼敓锟�?";
						String[] bbSP = bS[j].trim().split("\\|", -1);
						// System.out.println("as"+i+"= "+aS[i]);
						// System.out.println("bs" + j + "= " + bS[j]);
						// System.out.println("bs size= "+bbSP.length);
						// compute
						if (aaSP[0].equals(bbSP[0])) {
							coorelateScore = 0;
							// System.out.println("aaSP[0]= "+aaSP[0]);
							// System.out.println("bbSP[0]= "+bbSP[0]);
						} else {
//							if(aaSP[0].equals("02000006000000012016051799151304")){
							if(true){
//								 System.out.println("as" +
//								 bS[Integer.valueOf(aS[i])]);
//								 System.out.println("bS[j].trim() " +
//								 bS[j].trim());
							
							// Director
							// System.out.println("in");
							float iDirector = 0;
							String sDirector = "";
							if (aaSP[1].trim() == null | aaSP[1].trim().isEmpty() | bbSP[1].trim() == null
									| bbSP[1].trim().isEmpty()) {
								iDirector = 0f;
							} else {
								sDirector = RCR.CoSinTwoString(aaSP[1].trim(), bbSP[1].trim(), "Director");
								if (sDirector.trim().length() <= 1) {
									iDirector = 0;
								} else {
									String[] stringTemp = sDirector.trim().split("###");
									iDirector = Float.valueOf(stringTemp[0]);
									sDirector = stringTemp[1];
								}
							}
							// System.out.println("iDirector= "+iDirector);
							// Casts
							float iCasts = 0;
							String sCasts = "";
							if (aaSP[2].trim() == null | aaSP[2].trim().isEmpty() | bbSP[2].trim() == null
									| bbSP[2].trim().isEmpty()) {
								iCasts = 0;
							} else {
								sCasts = RCR.CoSinTwoString(aaSP[2].trim(), bbSP[2].trim(), "Casts");
								if (sCasts.trim().length() <= 1) {
									iCasts = 0;
								} else {
									String[] stringTemp = sCasts.trim().split("###");
									iCasts = Float.valueOf(stringTemp[0]);
									sCasts = stringTemp[1];
								}
							}
							// System.out.println("iCasts= "+iCasts);

							// SplitWord
							float iSplitWord = 0;
							String sSplitWord = "";
							if (aaSP[3].trim() == null | aaSP[3].trim().isEmpty() | bbSP[3].trim() == null
									| bbSP[3].trim().isEmpty()) {
								iSplitWord = 0;
							} else {
								sSplitWord = RCR.CoSinTwoString(aaSP[3].trim(), bbSP[3].trim(), "Title");
								if (sSplitWord.trim().length() <= 1) {
									iSplitWord = 0;
								} else {
									String[] stringTemp = sSplitWord.trim().split("###");
									iSplitWord = Float.valueOf(stringTemp[0]);
									sSplitWord = stringTemp[1];
								}
							}
							// System.out.println("SplitWord= "+iSplitWord);
							// Year
							float iYear = 0;
							String sYear = "";
							if (aaSP[4].trim() == null | aaSP[4].trim().isEmpty() | bbSP[4].trim() == null
									| bbSP[4].trim().isEmpty()) {
								iYear = 0;
							} else {
								int tempYear = Math
										.abs(Integer.valueOf(aaSP[4].trim()) - Integer.valueOf(bbSP[4].trim())) * 2;
								if (tempYear > 50) {
									iYear = 0;
								} else {
									iYear = 50 - tempYear;
									sYear = "SCORE(Year) = " + iYear / 1000;
								}
							}
							// System.out.println("iYear= "+iYear);
							// Genres
							float iGenres = 0;
							String sGenres = "";
							if (aaSP[5].trim() == null | aaSP[5].trim().isEmpty() | bbSP[5].trim() == null
									| bbSP[5].trim().isEmpty()) {
								iGenres = 0;
							} else {
								sGenres = RCR.CoSinTwoString(aaSP[5].trim(), bbSP[5].trim(), "Genres");
								if (sGenres.trim().length() <= 1) {
									iGenres = 0;
								} else {
									String[] stringTemp = sGenres.trim().split("###");
									iGenres = Float.valueOf(stringTemp[0]);
									sGenres = stringTemp[1];
								}
							}
							// System.out.println("iGenres= "+iGenres);

							// Country
							float iCountry = 0;
							String sCountry = "";
							if (aaSP[6].trim() == null | aaSP[6].trim().isEmpty() | bbSP[6].trim() == null
									| bbSP[6].trim().isEmpty()) {
							} else {
								sCountry = RCR.CoSinTwoString(aaSP[6].trim(), bbSP[6].trim(), "Country");
								if (sCountry.trim().length() <= 1) {
									iCountry = 0;
								} else {
									String[] stringTemp = sCountry.trim().split("###");
									iCountry = Float.valueOf(stringTemp[0]);
									sCountry = stringTemp[1];
								}
							}
							// System.out.println("iCountry= "+iCountry);
							// Language
							float iLanguage = 0;
							String sLanguage = "";
							if (aaSP[7].trim() == null | aaSP[7].trim().isEmpty() | bbSP[7].trim() == null
									| bbSP[7].trim().isEmpty()) {
								iLanguage = 0;
							} else {
								sLanguage = RCR.CoSinTwoString(aaSP[7].trim(), bbSP[7].trim(), "Language");
								if (sLanguage.trim().length() <= 1) {
									iLanguage = 0;
								} else {
									String[] stringTemp = sLanguage.trim().split("###");
									iLanguage = Float.valueOf(stringTemp[0]);
									sLanguage = stringTemp[1];
								}
							}
							// System.out.println("iLanguage= "+iLanguage);

							// Writers
							float iWriters = 0;
							String sWriters = "";
							if (aaSP[8].trim() == null | aaSP[8].trim().isEmpty() | bbSP[8].trim() == null
									| bbSP[8].trim().isEmpty()) {
								iWriters = 0;
							} else {
								sWriters = RCR.CoSinTwoString(aaSP[8].trim(), bbSP[8].trim(), "Writers");
								if (sWriters.trim().length() <= 1) {
									iWriters = 0;
								} else {
									String[] stringTemp = sWriters.trim().split("###");
									iWriters = Float.valueOf(stringTemp[0]);
									sWriters = stringTemp[1];
								}
							}
							
							float iCat = 0;
							String sCat = "";
							if(aaSP[17].trim().equals(bbSP[17].trim())){
								iCat = 100;
								sCat = "COS(Cat) = 1.0";
							}
							
							
							// iDirector * 0.25 +
							// iCasts * 0.2 +
							// iSplitWord * 0.3 +
							// iGenres * 0.05
							// iCountry * 0.05 +
							// iLanguage * 0.05 +
							// iWriters * 0.05
							// iYear 0.05
							// iCat 0.1
							
							
							
							coorelateScore = (iDirector * 0.25 + iCasts * 0.2 + iSplitWord * 0.3 + iGenres * 0.05
									+ iCountry * 0.05 + iLanguage * 0.05 + iWriters * 0.05) + iYear + iCat;
							String comment = "";
							if (iDirector > 0) {
								comment += sDirector + "<br> ";
							}
							if (iCasts > 0) {
								comment += sCasts + "<br> ";
							}
							if (iSplitWord > 0) {
								comment += sSplitWord + "<br> ";
							}
							if (iYear > 0) {
								comment += sYear + "<br> ";
							}
							if (iGenres > 0) {
								comment += sGenres + "<br> ";
							}
							if (iCountry > 0) {
								comment += sCountry + "<br> ";
							}
							if (iLanguage > 0) {
								comment += sLanguage + "<br> ";
							}
							if (iWriters > 0) {
								comment += sWriters + "<br> ";
							}
							if (iCat > 0){
								comment += sCat + "<br> ";
							}

							comment += "Total " + "[Director]" + (iDirector / 1000) + "*0.250" + "+" + "[Casts]"
									+ (iCasts / 1000) + "*0.200" + "+" + "[Title]" + (iSplitWord / 1000) + "*0.300"
									+ "+" + "[Year]" + iYear/1000 + "+" + "[Genres]" + (iGenres / 1000)
									+ "*0.050" + "+" + "[Country]" + (iCountry / 1000) + "*0.050" + "+" + "[Language]"
									+ (iLanguage / 1000) + "*0.050" + "+" + "[Writers]" + (iWriters / 1000) + "*0.050" 
									+ "+" + "[Cat]" + "1.0" + "*0.100";

							comment = aaSP[11].trim() + "|" + aaSP[0].trim() + "|" + bbSP[11].trim() + "|"
									+ bbSP[0].trim() + "|" + aaSP[12].trim() + "|" + bbSP[12].trim() + "|"
									+ bbSP[9].trim() + "|" + bbSP[14].trim() + "|" + bbSP[15].trim() + "|"
									+ (int) coorelateScore + "|" + bbSP[10].trim() + "|" + comment.trim() + "|"
									+ bbSP[16].trim() + "|" + bbSP[13].trim();

							// System.out.println("image=
							// "+bbSP[10].trim());

							Record record = new Record((int) coorelateScore, aaSP[0].trim() + "|" + bbSP[0].trim(),
									comment);
							// System.out.println("record= "+
							// coorelateScore+ " "+ aaSP[0].trim() + "|" +
							// bbSP[0].trim()+" "+ comment);

							if (aaSP[16].trim().equals("0")) {
								ScoreHDL.add(record);
							} else if (aaSP[16].trim().equals("1")) {
								ScoreHDH.add(record);
							}
							// System.out.println("size H= " + ScoreHDH.size());
							// System.out.println("size L= " + ScoreHDL.size());
							// if (aaSP[16].trim().equals("0") &&
							// bbSP[16].trim().equals("0")) {
							// ScoreHDL.add(record);
							// } else if (aaSP[16].trim().equals("1")) {
							// ScoreHDH.add(record);
							// }
							// } else if (aaSP[16].trim().equals("1") &&
							// bbSP[16].trim().equals("1")) {
							// ScoreHDH.add(record);
							// }

							// if (bbSP[16].trim().equals("0")) {
							// ScoreHDL.add(record);
							// }
							// ScoreHDH.add(record);

							// if (bbSP[16].trim().equals("0")) {
							// ScoreHDL.add(record);
							// } else if (bbSP[16].trim().equals("1")) {
							// ScoreHDH.add(record);
							 }
						}
					}

					if (aaSP[16].trim().equals("1")) {
						Collections.sort(ScoreHDH, comparator); // sorting
																// the
																// arraylist
						if (ScoreHDH.size() <= pLen) {
							for (int ip = 0; ip < ScoreHDH.size(); ip++) {
								// if (ScoreHDH.get(ip).iScore > 0) {
								keyText.set(ScoreHDH.get(ip).sString.trim());
								valueText.set(String.valueOf(ScoreHDH.get(ip).iScore).trim() + "###"
										+ ScoreHDH.get(ip).sComment.trim());
								context.write(keyText, valueText);
//								System.out.println("kvH= " + keyText + "###" + valueText);
								// }
							}
						} else {
							for (int ip = 0; ip < pLen; ip++) {
								// if (ScoreHDH.get(ip).iScore > 0) {
								keyText.set(ScoreHDH.get(ip).sString.trim());
								valueText.set(String.valueOf(ScoreHDH.get(ip).iScore).trim() + "###"
										+ ScoreHDH.get(ip).sComment.trim());
								context.write(keyText, valueText);
//								System.out.println("kvH= " + keyText + "###" + valueText);
								// }
							}
						}
					}

					if (aaSP[16].trim().equals("0")) {
						Collections.sort(ScoreHDL, comparator); // sorting
																// the
						// arraylist
						if (ScoreHDL.size() <= pLen) {
							for (int ip = 0; ip < ScoreHDL.size(); ip++) {
								// if (ScoreHDL.get(ip).iScore > 0) {
								keyText.set(ScoreHDL.get(ip).sString.trim());
								valueText.set(String.valueOf(ScoreHDL.get(ip).iScore).trim() + "###"
										+ ScoreHDL.get(ip).sComment.trim());
								context.write(keyText, valueText);
//								System.out.println("kvL= " + keyText + "###" + valueText);
								// }
							}
						} else {
							for (int ip = 0; ip < pLen; ip++) {
								// if (ScoreHDL.get(ip).iScore > 0) {
								keyText.set(ScoreHDL.get(ip).sString.trim());
								valueText.set(String.valueOf(ScoreHDL.get(ip).iScore).trim() + "###"
										+ ScoreHDL.get(ip).sComment.trim());
								context.write(keyText, valueText);
//								System.out.println("kvL= " + keyText + "###" + valueText);
							}
						}
					}
					// }
				}
			}
		}
		// System.out.println("Map3 finished");
	}

	class Record {
		int iScore;
		String sString;
		String sComment;

		public Record(int iScore, String sString, String sComment) {
			this.iScore = iScore;
			this.sString = sString;
			this.sComment = sComment;
		}

	}

	public String CoSinTwoString(String aa, String bb, String type) {
		// output score###string
		String[] aaaSP = aa.trim().split(" |,|，|\\|");
		String[] bbbSP = bb.trim().split(" |,|，|\\|");
		String commentTemp = "";
		if (aaaSP.length == 0 || bbbSP.length == 0) {
			commentTemp = "0";
			return commentTemp;
		} else {
			int[] positionScore = new int[] { 5, 4, 3, 2, 2, 1, 1, 1, 1, 1 };

			ArrayList<Integer> PSA = new ArrayList<>();
			ArrayList<Integer> PSB = new ArrayList<>();
			ArrayList<Integer> PSAp = new ArrayList<>();
			ArrayList<Integer> PSBp = new ArrayList<>();
			// ArrayList<Integer> PSAstring = new ArrayList<>();
			// ArrayList<Integer> PSBstring = new ArrayList<>();
			ArrayList<Integer> PSAD = new ArrayList<>();
			ArrayList<Integer> PSBD = new ArrayList<>();

			int alength = 10;
			int blength = 10;
			if (aaaSP.length < 10) {
				alength = aaaSP.length;
			}
			if (bbbSP.length < 10) {
				blength = bbbSP.length;
			}

			float scoreDL = 0; // down left
			float scoreDR = 0; // down right
			for (int d = 0; d < alength; d++) {
				PSAD.add(positionScore[d]);
				scoreDL += Math.pow(PSAD.get(d), 2);
			}
			for (int d = 0; d < blength; d++) {
				PSBD.add(positionScore[d]);
				scoreDR += Math.pow(PSBD.get(d), 2);
			}

			for (int m = 0; m < alength; m++) {
				for (int n = 0; n < blength; n++) {
					if (aaaSP[m].trim().equals(bbbSP[n].trim()) && aaaSP[m].trim().length() > 0) {
						PSA.add(positionScore[m]);
						PSB.add(positionScore[n]);
						PSAp.add(m);
						PSBp.add(n);
						break;
					}
				}
			}

			// for (int t1=0; t1<PSA.size(); t1++){
			// System.out.println("psa["+t1+"]= "+ PSA.get(t1));
			// }
			// for (int t1=0; t1<PSA.size(); t1++){
			// System.out.println("psb["+t1+"]= "+ PSB.get(t1));
			// }

			float scoreU = 0; // upper
			float score = 0;
			commentTemp += type + " ";
			if (PSA.size() > 0) {
				// System.out.println("debug");
				for (int k = 0; k < PSA.size(); k++) {
					scoreU += PSA.get(k) * PSB.get(k);
					if (k == PSA.size() - 1) {
						commentTemp += "[" + PSAp.get(k) + " " + aaaSP[PSAp.get(k)] + " " + PSA.get(k) + "]" + "["
								+ PSBp.get(k) + " " + bbbSP[PSBp.get(k)] + " " + PSB.get(k) + "]";
					} else {
						commentTemp += "[" + PSAp.get(k) + " " + aaaSP[PSAp.get(k)] + " " + PSA.get(k) + "]" + "["
								+ PSBp.get(k) + " " + bbbSP[PSBp.get(k)] + " " + PSB.get(k) + "] + ";
					}
				}
			}
			score = (float) (scoreU * 1000 / (Math.sqrt(scoreDL) * Math.sqrt(scoreDR)));
//			commentTemp += type + " [sum]/[sqrt(" + scoreDL + " * " + scoreDR + ")] = [" + score / 1000 + "]";
			commentTemp += " COS("+type+") = " + score / 1000;
			if ((int) score > 0) {
				// System.out.println("return= " + String.valueOf(score) + "###"
				// + commentTemp);
				return String.valueOf(score) + "###" + commentTemp;
			} else {
				return "0";
			}
		}
	}
}
