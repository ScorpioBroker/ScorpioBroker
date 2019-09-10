/*** Begin copyright statement *************************************************
 * LeafEngine (eu.nec.leaf.commons)
 * 
 * File: StringUtils.java
 * 
 * NEC Display Solutions Europe GmbH  PROPRIETARY INFORMATION
 * 
 * This software is supplied under the terms of a license agreement
 * or nondisclosure agreement with NEC Display Solutions Europe GmbH
 * and may not be copied or disclosed except in accordance with the
 * terms of that agreement. The software and its source code contain
 * valuable  trade secrets and confidential information which have to
 * be maintained in confidence.
 * 
 * Any unauthorized publication, transfer to third parties or
 * duplication of the object or source code - either totally or in
 * part - is prohibited.
 * 
 * Copyright (c) 2010-2013 NEC Display Solutions Europe GmbH
 * All Rights Reserved.
 * 
 * Authors:
 *  the leafengine team at NEC Laboratories Europe Ltd.
 *  e-mail: leaf@neclab.eu
 * 
 * NEC Display Solutions Europe GmbH DISCLAIMS ALL WARRANTIES,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE AND THE WARRANTY AGAINST LATENT
 * DEFECTS, WITH RESPECT TO THE PROGRAM AND THE
 * ACCOMPANYING DOCUMENTATION.
 * 
 * No Liability For Consequential Damages IN NO EVENT SHALL NEC
 * Display Solutions Europe GmbH, NEC Corporation OR ANY OF ITS
 * SUBSIDIARIES BE LIABLE FOR ANY DAMAGES WHATSOEVER (INCLUDING,
 * WITHOUT LIMITATION, DAMAGES FOR LOSS OF BUSINESS PROFITS,
 * BUSINESS INTERRUPTION, LOSS OF INFORMATION, OR OTHER
 * PECUNIARY LOSS AND INDIRECT, CONSEQUENTIAL, INCIDENTAL,
 * ECONOMIC OR PUNITIVE DAMAGES) ARISING OUT OF THE USE OF
 * OR INABILITY TO USE THIS PROGRAM, EVEN IF NEC Display
 * Solutions Europe GmbH HAS BEEN ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGES.
 * 
 * THIS HEADER MAY NOT BE EXTRACTED OR MODIFIED IN ANY WAY.
 ******************************************************************************/
package eu.neclab.ngsildbroker.commons.tools;

/**
 * A Utility class with common functions for Strings.
 * 
 * @author the leafengine team
 * 
 */
public final class StringUtils {

	private StringUtils() {
		// Prevent instantiation
	}

	/**
	 * Construct a string in the form of "x hour(s) y minute(s) z minute(s)"
	 * from a time duration in milliseconds. Some examples of possible output :<br/>
	 * <ul>
	 * <li>1 hour 2 minutes</li>
	 * <li>1 hour 2 minutes 1 second</li>
	 * <li>3 hours 1 minutes 2 seconds</li>
	 * <li>0 second</li>
	 * <li>4 hours</li>
	 * <li>5 hours 1 second</li>
	 * </ul>
	 * 
	 * @param msTime
	 *            time in milliseconds
	 * @return the formatted time string
	 */
	public static String formatDurationInMs(long msTime) {
		int sTime = (int) msTime / 1000;
		int seconds = sTime % 60;
		int minutes = (sTime % 3600) / 60;
		int hours = sTime / 3600;
		StringBuilder sBuilder = new StringBuilder();

		if (hours > 1) {
			sBuilder.append(hours + " hours ");
		} else if (hours == 1) {
			sBuilder.append("1 hour ");
		}

		if (minutes > 1) {
			sBuilder.append(minutes + " minutes ");
		} else if (minutes == 1) {
			sBuilder.append("1 minute ");
		}

		if (seconds > 1) {
			sBuilder.append(seconds + " seconds ");
		} else if (seconds == 1) {
			sBuilder.append("1 second ");
		} else if (sBuilder.length() == 0) {
			sBuilder.append("less than a second ");
		}

		return sBuilder.toString().trim();
	}

	/**
	 * Check if a string has value and is not an empty string.
	 * 
	 * @param input
	 *            the string to check
	 * @return true if it's set and not empty
	 */
	public static boolean isSet(String input) {
		return input != null && !(input.isEmpty() || input.trim().isEmpty());
	}

	/**
	 * Count how many a char appears in a string. It returns 0 if the string is
	 * not set.
	 * 
	 * @param text
	 *            string to search
	 * @param someCharacter
	 *            character to count
	 * @return the number of times the char appears in the text
	 */
	public static int countOccurrences(String text, char someCharacter) {
		if (!isSet(text)) {
			return 0;
		}
		int count = 0;
		for (int i = 0; i < text.length(); i++) {
			if (text.charAt(i) == someCharacter) {
				count++;
			}
		}
		return count;
	}

	/**
	 * Apply trimming any string on a string.
	 * 
	 * @param source
	 *            String source
	 * @param removed
	 *            Removed string
	 * @return String which is not started and ended with a removed string.
	 */
	public static String trimWithString(String source, String removed) {
		if (!isSet(removed) || !isSet(removed)) {
			return source;
		}
		String ret = source;
		while (ret.startsWith(removed)) {
			ret = ret.substring(removed.length());
		}
		while (ret.endsWith(removed)) {
			ret = ret.substring(0, ret.length() - removed.length());
		}

		return ret;
	}

}
