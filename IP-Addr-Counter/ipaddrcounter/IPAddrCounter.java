import java.io.*;
import java.util.*;
import java.nio.file.*;
import java.util.zip.*;


public class IPAddrCounter {

    public static void main(String[] args) {
        
        try {
              IPAddrCounter ipcs= new IPAddrCounter();    

              ipcs.SetParamsFromCmdStrings(args);
              ipcs.run();
        } 
        catch(Exception e){
          System.out.println(e.getMessage());    
        }
    }
    
	private void ShowHelp(){
        String helpUTF8 = new String(
        "Подсчет количества уникальных IP адресов в файле.\n\n"+
        "IPAddrCounter -f [путь]имяФайла [-z [путь]имяZipФайла] [-n число потоков] [-b размер буфера] [-a] [-s] [-p] [-h]\n"+
        "      -f имя файла с IP адресами\n"+
        "      -z имя ZIP файла c файлом IP адресов\n"+
        "      -n количество потоков для парсинга строк с IP адресами\n"+        
        "      -b размер буфера чтения, Kb\n"+        
        "      -a выделение памяти в начале работы утилиты\n"+
        "      -s не выводить статистику хода работ процесса\n"+
        "      -p вывести все одиночные адреса\n"+
        "      -h help");
        try {
            System.out.println( new String(helpUTF8.getBytes(),"utf-8") );
        }
        catch (UnsupportedEncodingException e){
            System.out.println("error: "+e.getMessage());
        }
         System.exit(0); 
    }

    private void SetParamsFromCmdStrings(String[] args) {
        
        if (args.length == 0) { ShowHelp();}

        for(int i=0; i<args.length; i++) {
            switch ( args[i] ) {
                case "-f": nameFile    = args[++i]; break;
                case "-z": nameFileZip = args[++i];     break;
                case "-n": countThreadsMax= Integer.parseInt(args[++i]);  break;
                case "-b": sizeBufferMax  = Integer.parseInt(args[++i]) *1024; break;                
                case "-a": cmdAllocateMemoryPages=true; break;
                case "-s": cmdOnShowTelemetry = false;  break;
                case "-p": cmdPrintSingleIP   = true;   break;
//                    -o имя Файла, куда складывать результаты                 
                case "-h": ShowHelp(); return;
                default  : ShowHelp(); return; 
            }
        }
    }        
    
    private static Timer   timerStatistics;  
    private static boolean flgPauseShowTelemetry = true; // выводить телеметрию по таймеру JMeter
    private static boolean cmdOnShowTelemetry    = true; // включить вывод телеметрии
    private        boolean cmdAllocateMemoryPages= false;   
    private        boolean cmdPrintSingleIP      = false;   
    
    private FileLinesStream fs;    
    private String       nameFile = ""; 
    private String       nameFileZip = "";

    private int          sizeBufferMax   = 64*1024;
    private int          countThreadsMax = 2;
    
    private long         tmStartWork=0; 
    
    private void run()  throws IOException, InterruptedException {

        fs = new FileLinesStream(nameFile, nameFileZip);

        IPCounterReadStream[]  thIPStream = new IPCounterReadStream[countThreadsMax];

        IPCounterSingle ipcs  = new IPCounterSingle();
        if(cmdAllocateMemoryPages) ipcs.AllocateMemoryPages(); 
       
        timerStatistics    = new Timer();  
        timerStatistics.schedule( 
            new TimerTask() {  @Override  public void run() { OutputStatistics();} }, 0, 1* 1000 );

        tmStartWork = System.nanoTime();
        flgPauseShowTelemetry = !cmdOnShowTelemetry;
        
        System.out.printf("\r %s", Statistics());
        try {

            for( int i=0;i<countThreadsMax;i++) {
                 thIPStream[i]=new IPCounterReadStream(fs, ipcs,"IPCnt00"+i);
                 thIPStream[i].sizeBufferMax=sizeBufferMax;
                 thIPStream[i].start();
            }

            for( int i=0;i<countThreadsMax;i++) {
                thIPStream[i].join();
            }
        }
        catch(InterruptedException e){System.out.println("Error "+e.getMessage());}
        catch(Exception e){System.out.println("Error "+e.getMessage()); e.printStackTrace();}

        flgPauseShowTelemetry   = true;
        timerStatistics.cancel();

        System.out.printf("\r %s\n", Statistics());
        
        ipcs.CalcSingleIP(cmdPrintSingleIP); //расчет статистики по адресам
        System.out.println("\nResult: "+ipcs.Result());
        
        try {            
           fs.close();
        }
        catch(Exception e){
            System.out.println( e.getMessage() );
        } 
    }

	class IPCounterReadStream extends Thread {        
    
        private FileLinesStream fs;
        private IPCounterSingle ipcs;
        private String nameThread;
        
        public  int sizeBufferMax=64*1024; // размер буфера
        public  long totalThRead=0;        // всего считано потоком
        public  int bufRead=0;             // считано в буфер
        public  long countThIP=0;          // найдено IP адресов в потоке/файле
    
        IPCounterReadStream (FileLinesStream fs, IPCounterSingle ipcs, String nameThread) {
            super(nameThread);
            this.fs=fs;
            this.ipcs=ipcs;
            this.nameThread=nameThread;
        }
        
        public void run() {
            
            byte[] buffer = new byte[sizeBufferMax];
            int    ls_max=1024;
            long[] ls = new long[ls_max];
            int ils=0;
            
            int iBuffer=0;                    

            boolean flgBadIP=false;            
            int  IPx=0;
            long ip4=0;            
            int lenIP=0;
            int pIP=0;
            byte c;    
            
            countThIP=0; 
            try{
                while( (bufRead = fs.readLines(buffer, sizeBufferMax)) > 0) {
                    if (bufRead == -1) break;
                    totalThRead+=bufRead;
                    pIP=0;
                    ils=0;
                    for( iBuffer=0; 0 < (bufRead-iBuffer); ) {
                        IPx=0;
                        ip4=0;
                        
                        while( iBuffer < bufRead ) {
                            c= buffer[iBuffer++];
                            if ( !('0'<=c && c<='9') ) {                        
                               if(  c == '.' ) {ip4=(ip4<<8)+IPx; IPx=0; continue; }
                               if( (c == '\r') || (c ==' ') )  continue;
                               if( (c == '\n') || (c == 0))    break;
                            }  
                            IPx = IPx*10 +(c - '0');
                            if( IPx>255 ) flgBadIP=true;
                            lenIP++;
                        }
                        ip4=(ip4<<8)+IPx;                       
                       
                        if( flgBadIP ) { 
                            flgBadIP =false; 
                            byte[] out=new byte[(iBuffer-pIP)+1];
                            System.arraycopy(buffer,pIP,out,0,(iBuffer-pIP));
                            String s= new String(out);
                            System.out.printf("BAD IP4:%s\n", s);
                            continue;
                        }
                        
                        if(lenIP==0) continue;
                        lenIP=0;
                        pIP=iBuffer;
                        countThIP++;

                        ls[ils++]=ip4;
                        if(ils>=ls_max) { ipcs.Add( ls, ils ); ils=0; }
                    }   
                    ipcs.Add( ls, ils );
                    ils=0;
                }            
            }
            catch(IOException e){System.out.println(e.getMessage());}
            catch(InterruptedException e){System.out.println(e.getMessage());}
            catch(Exception e){System.out.println(e.getMessage());}
        }
    }
    
	static class IPCounterSingle {
        //  Адрес IPv4 описывается как A.B.C.D .
        //  Счетчики хранятся на страницах массивов tabIP4 и tabIP4b.
        
        // Счетчики
        public long cntIP=0;     //всего IP адресов добавлено в счетчик
        public long cntIP_1=0,   //всего одиночных IP адресов
                    cntIP_N=0,   //всего не одиночных IP адресов
                    cntIP_all=0; //всего IP адресов просмотрено

        int[]  tyCD = new int[32]; // битовая маска 

        int[][] tabIP4  = new int[256 * 256][]; //таблица признака одиночных адресов сети
        int[][] tabIP4b = new int[256 * 256][]; //таблица признака не одиночных адресов сети

        public IPCounterSingle() {
            for(int i=0; i<32; i++) tyCD[i] = (0x00000001 << i);  // заполняем битовую маску
        }

        public boolean AllocateMemoryPages() {
            for (int iAB = 0; iAB < 256 * 256; iAB++) {
                tabIP4[iAB]  = new int[8*256];
                tabIP4b[iAB] = new int[8*256];
            }
            return true;
        }
        
        synchronized void Add( long[] ls, int cls ) {
            for(int i=0;i<cls;i++) Add(ls[i]);
        }
        
        private void Add( long ip4 ) {
            // ip:*.*.*.* -> A.B.C.D -> AB.CD
            int ipAB, ipCD; // 
            int xCD, yCD;   // положение части адреса CD на странице( слово:бит_В_слове )
            int[] pgAB;     // страница части адреса AB с адресами CD 
            
            ipAB = (int)((ip4>>16)& 0x0FFFF);  //ipAB = ip[0]*256 +ip[1]; //A*256 +B;
            ipCD = (int)( ip4     & 0x0FFFF);  //ipCD = ip[2]*256 +ip[3]; //C*256 +D;
            
            pgAB=tabIP4[ipAB];
            
            // Добавить страницу с CD для адреса AB в основную таблицу
            if ( pgAB == null ) tabIP4[ipAB]=pgAB=new int[8*256];
            
            // расчет положения счетчика на странице( слово:бит_В_слове )
            xCD = ipCD / 32;    
            yCD = tyCD[ipCD%32];   //yCD = ((0x00000001) << (ipCD % 32));
           
            // добавить адрес в счетчик
            if ((pgAB[xCD] & yCD) == 0) {
                pgAB[xCD] |= yCD; // учет первого ip
            } else {
                // учет ip если более одного раза
                 // Добавить страницу для CD если более 1 адреса
                if (tabIP4b[ipAB] == null) tabIP4b[ipAB] = new int[8*256];
                tabIP4b[ipAB][xCD] |= yCD; 
            }
            cntIP++;
        }
        
        public void CalcSingleIP(boolean bPrnSingleIP) {  
       
            cntIP_1=cntIP_N=cntIP_all=0;
            int[] pgCD;  // страница с адресами CD 
            int[] pgCDb; // страница с адресами CD 
            int iAB,iCD;
            int xCD,yCD;
            for(iAB=0;iAB<256*256;iAB++) {
                pgCD  =tabIP4[iAB];
                pgCDb =tabIP4b[iAB];
                if( pgCD == null ) continue;
                
                for( iCD=0;iCD<256*256;iCD++) {
                    xCD=iCD/32;
                    yCD=tyCD[iCD%32];  //int yCD=0x00000001<<(iCD%32);
                    
                    if( (pgCD[xCD] & yCD) == 0 ) {
                        continue;
                    } else if( pgCDb == null ) {
                        cntIP_1++; 
                        if(bPrnSingleIP) System.out.println("ip:"+Long2IP(iAB*256*256+iCD));
                    } else if( (pgCDb[xCD] & yCD) == 0 ){
                        cntIP_1++;
                        if(bPrnSingleIP) System.out.println("ip:"+Long2IP(iAB*256*256+iCD));
                    } else {
                        cntIP_N++;
                    }
                         
                    cntIP_all++;            
                }
            }
        }
        
        public static String Long2IP(long ip4) {
            String rt= String.format("%d.%d.%d.%d",
                                  ((ip4>>8+8+8)&0x00FF),((ip4>>  8+8)&0x00FF),
                                  ((ip4>>    8)&0x00FF),((ip4       )&0x00FF) ); 
            return rt;
        }

        public String Result(){
            String s =String.format("1:%d N:%d all:%d (%d)\n", cntIP_1, cntIP_N, cntIP_all, cntIP);
            return s;
        }
	}
	
    public class FileLinesStream {

        String    nameFile = "";
        String    nameFileZip = "";

        ZipFile     zFile=null;      
        BufferedInputStream bis=null;
        
        public long szFile;
        public long sizeFile=0;
        public long lenRead=0;
        public int  len=0;
        
        //буфер с последней разорванной строкой адреса
        public int    buffer_stub_max=127;
        public byte[] buffer_stub= new byte[buffer_stub_max]; 
        public int    buffer_stub_len=0;
        
        FileLinesStream (String nameFile, String nameFileZip) {
            if(nameFileZip!="")  openZip(nameFile, nameFileZip);
            else                 open(nameFile);            
        }

        public void open(String nameFile) {
            this.nameFile=nameFile;
            try {
                File fl=new File(nameFile); 
                szFile=fl.length();
                bis = new BufferedInputStream(new FileInputStream(nameFile));    
            } 
            catch(Exception e){
                  throw new RuntimeException("Error open file: " +e.getMessage());
            }
            buffer_stub_len=0;
        }
        
        public void openZip(String nameFile,String nameFileZip) {
            this.nameFile=nameFile;
            this.nameFileZip=nameFileZip;
            try {
                zFile = new ZipFile(nameFileZip);
                ZipEntry eFile = zFile.getEntry(nameFile);
                if (eFile == null) throw new RuntimeException(nameFile+", "+nameFileZip);
                szFile = eFile.getSize();  // получим размер в байтах                
                bis = new BufferedInputStream(zFile.getInputStream(eFile));                    
            }
            catch(Exception e){
                  throw new RuntimeException("Error open file: " +e.getMessage());
            }
            buffer_stub_len=0;
        }        
        
        synchronized int readLines(byte[] buf, int lenMax) throws IOException, InterruptedException {   
            byte c;
            if(buffer_stub_len != 0)
                System.arraycopy(buffer_stub, 0,buf,0,buffer_stub_len);
        
            len=bis.read(buf, buffer_stub_len, lenMax-buffer_stub_len);
            if( len <= 0 ) {
                len=buffer_stub_len;
                if( len > 0 ) lenRead += len;
                buffer_stub_len=0;
                return len;
            }
            len+=buffer_stub_len;
            
            for(buffer_stub_len=0; buffer_stub_len<buffer_stub_max; buffer_stub_len++) {
                c=buf[len-buffer_stub_len -1];
                if(c=='\n') break;
            }
            System.arraycopy(buf, len-buffer_stub_len, buffer_stub, 0, buffer_stub_len);

            len     -= buffer_stub_len;
            lenRead += len;
            return len;
        }

        public void close()throws IOException, InterruptedException 
        {                                  
            if(zFile!=null) zFile.close();
            if(bis  !=null) bis.close();
        }
    }
    
    private void OutputStatistics() {

        if( flgPauseShowTelemetry ) return;
//      boolean flgPause=flgPauseShowTelemetry; //запомнить состояние
        flgPauseShowTelemetry=true;             //отключить до окончания текущего вывода
        if( fs.len > 0) 
            System.out.printf("\r %s", Statistics());
        flgPauseShowTelemetry=false;
    }    

    public String Statistics() {

        long tmWork, speed, tmForecastWork;

        tmWork = (System.nanoTime() - tmStartWork)/1000/1000;
        if( tmWork == 0 ) return ""; 
        
        speed = fs.lenRead/tmWork;
        tmForecastWork = fs.szFile/speed; 
        tmWork /=1000;
        tmForecastWork /=1000;
        
        String rt=String.format("%02d:%02d(%02d:%02d) %d Mb/s, %d(%d) Mb", 
               tmWork/60,tmWork%60, tmForecastWork/60,tmForecastWork%60,
               speed/1024,
               fs.lenRead/1024/1024, fs.szFile/1024/1024);

        return rt;
    }
}