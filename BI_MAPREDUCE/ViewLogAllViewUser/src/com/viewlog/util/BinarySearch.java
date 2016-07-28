package com.viewlog.util;
/**
* @author VitoLiao
* @version 2016��4��14�� ����3:37:46
*/
public class BinarySearch {   
    public static void main(String[] args) {  
        int[] src = new int[] {1, 3, 5, 7, 8, 9};   
       
    }  
  
    /** 
     * * ���ֲ����㷨 * * 
     *  
     * @param srcArray 
     *            �������� * 
     * @param des 
     *            ����Ԫ�� * 
     * @return des�������±꣬û�ҵ�����-1 
     */   
   public static int binarySearchint(int[] srcArray, int des){   
      
        int low = 0;   
        int high = srcArray.length-1;   
        while(low <= high) {   
            int middle = (low + high)/2;   
            if(des == srcArray[middle]) {   
                return middle;   
            }else if(des <srcArray[middle]) {   
                high = middle - 1;   
            }else {   
                low = middle + 1;   
            }  
        }  
        return -1;  
   }  
   
   public static String binarySearchString(String[] srcArray, String des){   
	      
       int low = 0;   
       int high = srcArray.length-1;   
       while(low <= high) {   
           int middle = (low + high)/2;   
           if(des.equals(srcArray[middle])) {   
               return srcArray[middle];   
           }else if(des.compareTo(srcArray[middle])<0) {   
               high = middle - 1;   
           }else {   
               low = middle + 1;   
           }  
       }  
       return  "0";  
  }  
       
      /**   
     *���ֲ����ض����������������е�λ��(�ݹ�)   
     *@paramdataset   
     *@paramdata   
     *@parambeginIndex   
     *@paramendIndex   
     *@returnindex   
     */  
    public static int binarySearch(int[] dataset,int data,int beginIndex,int endIndex){    
       int midIndex = (beginIndex+endIndex)/2;    
       if(data <dataset[beginIndex]||data>dataset[endIndex]||beginIndex>endIndex){  
           return -1;    
       }  
       if(data <dataset[midIndex]){    
           return binarySearch(dataset,data,beginIndex,midIndex-1);    
       }else if(data>dataset[midIndex]){    
           return binarySearch(dataset,data,midIndex+1,endIndex);    
       }else {    
           return midIndex;    
       }    
   }   
  
} 
