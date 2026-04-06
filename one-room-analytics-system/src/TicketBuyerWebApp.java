import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.Headers;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.text.NumberFormat;
import java.util.Locale;

public class TicketBuyerWebApp {


    private static final String[] CATEGORIES = {
            "Afrosoul", "Jazz", "Comedy", "Folk", "HipHop", "Reggae"
    };

    private static class ProcessedData {
        List<String> showNames;
        List<List<String>> showEmailsLists;
        List<List<String>> showPhoneLists;

        List<String> catAfroEmails;
        List<String> catJazzEmails;
        List<String> catComedyEmails;
        List<String> catPoetryEmails; // shown as Folk
        List<String> catHipHopEmails;
        List<String> catReggaeEmails;

        List<String> catAfroPhones;
        List<String> catJazzPhones;
        List<String> catComedyPhones;
        List<String> catPoetryPhones; // shown as Folk
        List<String> catHipHopPhones;
        List<String> catReggaePhones;

        List<String> contactEmails;
        List<String> contactPhones;
        List<List<String>> contactShows;

        // NEW: count-based metrics (no money assumptions)
        List<Integer> showTicketCounts; // tickets per show (rows)
        int[] catTicketCounts;          // tickets per category
        int totalTickets;               // all tickets across shows

        int[] timeBuckets;     // [Morning, Afternoon, Evening, Late/Other]
        int[] groupSizeBuckets; // [0, size1, size2, size3, size4, size5, size6plus]
    }

    public static void main(String[] args) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", 8080), 0);

        server.createContext("/", TicketBuyerWebApp::handleRoot);
        server.createContext("/showEmails", TicketBuyerWebApp::handleShowEmails);
        server.createContext("/showPhones", TicketBuyerWebApp::handleShowPhones);
        server.createContext("/categoryContacts", TicketBuyerWebApp::handleCategoryContacts);
        server.createContext("/categoryPhones", TicketBuyerWebApp::handleCategoryPhones);
        server.createContext("/logo", TicketBuyerWebApp::handleLogo);
        server.createContext("/upload", TicketBuyerWebApp::handleUpload);
        server.createContext("/exclusions", TicketBuyerWebApp::handleExclusions);
        server.createContext("/categories", TicketBuyerWebApp::handleCategories);
        server.createContext("/download", TicketBuyerWebApp::handleDownload);
        server.createContext("/dataInsights", TicketBuyerWebApp::handleDataInsights);

        server.setExecutor(null);
        server.start();
        System.out.println("The One Room Database Manager running on http://localhost:8080/");
    }

// ===================== DATA INSIGHTS PAGE (FULL + CHARTS) ======================
private static void handleDataInsights(HttpExchange ex) throws IOException {

    File folder = new File(".");
    ProcessedData data = processFolder(folder);

    int totalContacts = data.contactEmails.size();

    // ---------- RETENTION ----------
    int newCustomers = 0;
    int returningCustomers = 0;
    int totalVisits = 0;

    for (List<String> shows : data.contactShows) {
        int visits = (shows == null) ? 0 : shows.size();
        totalVisits += visits;

        if (visits <= 1) newCustomers++;
        else returningCustomers++;
    }

    double newRate = (totalContacts == 0) ? 0 :
            (newCustomers * 100.0 / totalContacts);

    double returningRate = (totalContacts == 0) ? 0 :
            (returningCustomers * 100.0 / totalContacts);

    double avgShowsPerContact = (totalContacts == 0) ? 0 :
            (double) totalVisits / totalContacts;

    // ---------- REVENUE ----------
    File[] csvs = folder.listFiles((d, n) ->
            n.toLowerCase().endsWith(".csv")
                    && !n.equalsIgnoreCase("contacts_with_shows.csv")
                    && !n.equalsIgnoreCase("all_contacts.csv"));

    if (csvs == null) csvs = new File[0];

    double totalRevenue = 0.0;
    double[] catRevenue = new double[6];
    Map<String, Double> yearRevenueMap = new HashMap<>();

    Map<String, Double> monthRevenue = new HashMap<>();
    Map<String, Double> seasonRevenue = new HashMap<>();

    int checkedIn = 0;
    int totalRows = 0;

    for (File f : csvs) {

        String baseName = f.getName().replace(".csv", "");
        int catIdx = categoryIndex(categoryGuess(baseName)) - 1;

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))) {

            String header = br.readLine();
            if (header == null) continue;

            List<String> cols = parseCsvLine(header);

            int iPrice   = findIndex(cols, "Price paid");
            int iDate    = findIndex(cols, "Purchase Date");
            int iChecked = findIndex(cols, "Checked In");
            int iEvent   = findIndex(cols, "EventDate");

            String line;

            while ((line = br.readLine()) != null) {

                List<String> row = parseCsvLine(line);

                double price = 0.0;

                if (iPrice >= 0 && iPrice < row.size()) {
                    try {
                        String cleaned = row.get(iPrice).replace("R", "").trim();
                        if (!cleaned.isEmpty()) {
                            price = Double.parseDouble(cleaned);
                        }
                    } catch (Exception ignored) {}
                }

                if (price > 0) {
                    totalRevenue += price;
                    if (catIdx >= 0 && catIdx < 6) {
                        catRevenue[catIdx] += price;
                    }
                }

                if (iDate >= 0 && iDate < row.size()) {
                    String pd = row.get(iDate);
                    if (pd != null && pd.length() >= 4) {
                        String year = pd.substring(0, 4);
                        yearRevenueMap.put(
                                year,
                                yearRevenueMap.getOrDefault(year, 0.0) + price
                        );
                    }
                }

                // MONTH + SEASON
                try {
                    if (iEvent >= 0 && iEvent < row.size()) {

                        String ed = row.get(iEvent);

                        if (ed != null && !ed.isEmpty()) {

                            String eDate = ed.split(" ")[0];
                            LocalDate event = LocalDate.parse(eDate);

                            String month = event.getMonth().toString();

                            monthRevenue.put(month,
                                    monthRevenue.getOrDefault(month, 0.0) + price);

                            int m = event.getMonthValue();
                            String season;

                            if (m == 12 || m == 1 || m == 2) season = "Summer";
                            else if (m <= 5) season = "Autumn";
                            else if (m <= 8) season = "Winter";
                            else season = "Spring";

                            seasonRevenue.put(season,
                                    seasonRevenue.getOrDefault(season, 0.0) + price);
                        }
                    }
                } catch (Exception ignored) {}

                if (iChecked >= 0 && iChecked < row.size()) {
                    if ("Yes".equalsIgnoreCase(row.get(iChecked))) {
                        checkedIn++;
                    }
                }

                totalRows++;
            }

        } catch (Exception ignored) {}
    }

    double attendanceRate = (totalRows == 0) ? 0 :
            (checkedIn * 100.0 / totalRows);

    // ---------- YEAR ----------
    List<String> years = new ArrayList<>(yearRevenueMap.keySet());
    Collections.sort(years);

    double[] yearRevenue = new double[years.size()];
    for (int i = 0; i < years.size(); i++) {
        yearRevenue[i] = yearRevenueMap.get(years.get(i));
    }

    // ---------- INSIGHTS ----------
    String bestYear = "-";
    double bestYearValue = 0;

    for (int i = 0; i < years.size(); i++) {
        if (yearRevenue[i] > bestYearValue) {
            bestYearValue = yearRevenue[i];
            bestYear = years.get(i);
        }
    }

    String bestMonth = "-", worstMonth = "-";
    double bestMonthValue = 0, worstMonthValue = Double.MAX_VALUE;

    for (Map.Entry<String, Double> e : monthRevenue.entrySet()) {
        if (e.getValue() > bestMonthValue) {
            bestMonthValue = e.getValue();
            bestMonth = e.getKey();
        }
        if (e.getValue() < worstMonthValue) {
            worstMonthValue = e.getValue();
            worstMonth = e.getKey();
        }
    }

    String bestSeason = "-";
    double bestSeasonValue = 0;

    for (Map.Entry<String, Double> e : seasonRevenue.entrySet()) {
        if (e.getValue() > bestSeasonValue) {
            bestSeasonValue = e.getValue();
            bestSeason = e.getKey();
        }
    }

    String[] categories = {"Afrosoul","Jazz","Comedy","Folk","HipHop","Reggae"};

    int bestCatIndex = 0;
    double bestCatValue = 0;

    for (int i = 0; i < catRevenue.length; i++) {
        if (catRevenue[i] > bestCatValue) {
            bestCatValue = catRevenue[i];
            bestCatIndex = i;
        }
    }

    String[] timeLabels = {"Morning","Afternoon","Evening","Late"};

    int maxTimeIndex = 0;
    for (int i = 1; i < data.timeBuckets.length; i++) {
        if (data.timeBuckets[i] > data.timeBuckets[maxTimeIndex]) {
            maxTimeIndex = i;
        }
    }

    int maxGroupIndex = 1;
    for (int i = 2; i < data.groupSizeBuckets.length; i++) {
        if (data.groupSizeBuckets[i] > data.groupSizeBuckets[maxGroupIndex]) {
            maxGroupIndex = i;
        }
    }

    // ---------- HTML ----------
    StringBuilder html = new StringBuilder();

    html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'>");
    html.append("<title>Data Insights</title>");
    html.append(sharedCss());

    html.append("<style>");
    html.append(".chart-container { max-width: 600px; margin: auto; }");
    html.append("canvas { max-height: 250px !important; }");
    html.append(".chart-note { font-size: 14px; color: #555; margin-top: 10px; }");
    html.append("</style>");

    html.append("<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>");
    html.append("</head><body><div class='wrapper'>");

    html.append("<div class='hero'><h1>Data Insights</h1><p>Financial and audience analytics</p></div>");

    // KPI
    html.append("<div class='card'><h2>Key Metrics</h2><ul>");
    html.append("<li>Total Revenue: ").append(formatCurrency(totalRevenue)).append("</li>");
    html.append("<li>Returning Audience: ").append(String.format("%.1f", returningRate)).append("%</li>");
    html.append("<li>New Audience: ").append(String.format("%.1f", newRate)).append("%</li>");
    html.append("<li>Attendance Rate: ").append(String.format("%.1f", attendanceRate)).append("%</li>");
    html.append("<li>Avg Shows per Person: ").append(String.format("%.2f", avgShowsPerContact)).append("</li>");
    html.append("</ul></div>");

    // Charts
    html.append("<div class='card'><h2>Revenue over time</h2><div class='chart-container'><canvas id='yearChart'></canvas></div>");
    html.append("<p class='chart-note'>Revenue peaked in ").append(bestYear)
            .append(" at ").append(formatCurrency(bestYearValue))
            .append(", representing the highest level of financial performance.</p></div>");

    html.append("<div class='card'><h2>Revenue by category</h2><div class='chart-container'><canvas id='catChart'></canvas></div>");
    html.append("<p class='chart-note'>Revenue concentration is highest in ")
            .append(categories[bestCatIndex]).append(" (")
            .append(formatCurrency(bestCatValue))
            .append(").</p></div>");

    html.append("<div class='card'><h2>Audience mix</h2><div class='chart-container'><canvas id='retentionChart'></canvas></div>");
    html.append("<p class='chart-note'>Returning: ")
            .append(String.format("%.1f", returningRate))
            .append("%, New: ").append(String.format("%.1f", newRate))
            .append("%.</p></div>");

    // TIME BEHAVIOUR
    html.append("<div class='card'><h2>Time behaviour</h2><div class='chart-container'><canvas id='timeChart'></canvas></div>");

    html.append("<p class='chart-note'>Peak booking time is ")
            .append(timeLabels[maxTimeIndex])
            .append(" with ")
            .append(data.timeBuckets[maxTimeIndex])
            .append(" bookings. ");

    html.append("Other periods: ");
    for (int i = 0; i < timeLabels.length; i++) {
        if (i != maxTimeIndex) {
            html.append(timeLabels[i])
                    .append(" (").append(data.timeBuckets[i]).append("), ");
        }
    }

    html.append("indicating demand is concentrated in the ")
            .append(timeLabels[maxTimeIndex])
            .append(" period, making it the most effective time for events.</p></div>");

    html.append("<div class='card'><h2>Group behaviour</h2><div class='chart-container'><canvas id='groupChart'></canvas></div></div>");

    html.append("<div class='card'><h2>Seasonality</h2><div class='chart-container'><canvas id='seasonChart'></canvas></div>");
    html.append("<p class='chart-note'>Peak month is ").append(bestMonth)
            .append(" (").append(formatCurrency(bestMonthValue))
            .append("), lowest is ").append(worstMonth)
            .append(" (").append(formatCurrency(worstMonthValue))
            .append("). Strongest season is ").append(bestSeason)
            .append(".</p></div>");

    // JS
    html.append("<script>");

    html.append("new Chart(document.getElementById('yearChart'), {type:'line', data:{labels:")
            .append(toJSStringArray(years)).append(", datasets:[{data:")
            .append(toJSDoubleArray(yearRevenue)).append("}]}});");

    html.append("new Chart(document.getElementById('catChart'), {type:'bar', data:{labels:['Afrosoul','Jazz','Comedy','Folk','HipHop','Reggae'], datasets:[{data:")
            .append(toJSDoubleArray(catRevenue)).append("}]}});");

    html.append("new Chart(document.getElementById('retentionChart'), {type:'pie', data:{labels:['New','Returning'], datasets:[{data:[")
            .append(newCustomers).append(",").append(returningCustomers).append("]}]}});");

    html.append("new Chart(document.getElementById('timeChart'), {type:'pie', data:{labels:['Morning','Afternoon','Evening','Late'], datasets:[{data:")
            .append(toJSIntArray(data.timeBuckets)).append("}]}});");

    html.append("new Chart(document.getElementById('groupChart'), {type:'bar', data:{labels:['1','2','3','4','5','6+'], datasets:[{data:")
            .append(toJSIntArray(data.groupSizeBuckets)).append(".slice(1)}]}});");

    html.append("new Chart(document.getElementById('seasonChart'), {type:'bar', data:{labels:['Summer','Autumn','Winter','Spring'], datasets:[{data:[")
            .append(seasonRevenue.getOrDefault("Summer",0.0)).append(",")
            .append(seasonRevenue.getOrDefault("Autumn",0.0)).append(",")
            .append(seasonRevenue.getOrDefault("Winter",0.0)).append(",")
            .append(seasonRevenue.getOrDefault("Spring",0.0))
            .append("]}]}});");

    html.append("</script>");

    html.append("</div></body></html>");

    sendHtml(ex, html.toString());
}


// ======================= HELPER: JS ARRAY BUILDERS ===========================

    private static String toJSStringArray(List<String> list) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            String v = (list.get(i) == null) ? "" : list.get(i);
            // basic quote escaping for JS
            v = v.replace("\\", "\\\\").replace("\"", "\\\"");
            sb.append("\"").append(v).append("\"");
            if (i < list.size() - 1) sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    private static String toJSIntArray(int[] arr) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < arr.length; i++) {
            sb.append(arr[i]);
            if (i < arr.length - 1) sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    private static String toJSIntArrayFromList(List<Integer> list) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            int v = (list.get(i) == null) ? 0 : list.get(i);
            sb.append(v);
            if (i < list.size() - 1) sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    private static String toJSDoubleArray(double[] arr) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < arr.length; i++) {
            sb.append(arr[i]);  // Java uses dot-decimal, safe for JS
            if (i < arr.length - 1) sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }



    // ========================== ROOT ===============================

    private static void handleRoot(HttpExchange ex) throws IOException {
        File folder = new File(".");
        ProcessedData data = processFolder(folder);

        List<String> overrideShows = new ArrayList<>();
        List<String> overrideCats  = new ArrayList<>();
        loadCategoryOverrides(folder, overrideShows, overrideCats);

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'>");
        html.append("<title>The One Room Database Manager</title>");
        html.append(rootCss());
        html.append("</head><body><div class='wrapper'>");

        // hero
        html.append("<div class='hero'><div class='hero-inner'>");
        html.append("<div class='logo-wrap'><img src='/logo' alt='The One Room logo'></div>");
        html.append("<div class='hero-text'>");
        html.append("<div class='hero-pill'>The One Room · Audience console</div>");
        html.append("<h1 class='hero-title'>The One Room Database Manager</h1>");
        html.append("<p class='hero-sub'>Central dashboard for ticket buyer emails and numbers by show and category.</p>");
        html.append("<p class='hero-meta'>Reads *.csv in this folder · Deduplicates contacts & numbers · Respects excluded emails</p>");
        html.append("</div></div></div>");

        html.append("<div class='grid'>");

        // shows card
        html.append("<div class='card'>");
        html.append("<h2>Shows in this folder</h2>");
        html.append("<p class='tagline'>Each row is a Webtickets CSV. Click to view email or number lists per show.</p>");

        if (data.showNames.isEmpty()) {
            html.append("<p class='muted'>No CSV files found here. Drop your Webtickets CSV exports into this folder, then refresh.</p>");
        } else {
            html.append("<table><thead><tr>");
            html.append("<th>#</th><th>Show</th><th>Category</th><th>Emails (unique)</th><th>Numbers (unique)</th><th>View</th>");
            html.append("</tr></thead><tbody>");
            for (int i = 0; i < data.showNames.size(); i++) {
                String show = data.showNames.get(i);
                String cat  = categoryForShow(show, overrideShows, overrideCats);
                int emailCount = data.showEmailsLists.get(i).size();
                int phoneCount = uniqueNonEmptyPhones(data.showPhoneLists.get(i)).size();

                html.append("<tr>");
                html.append("<td>").append(i + 1).append("</td>");
                html.append("<td>").append(escapeHtml(show)).append("</td>");
                html.append("<td><span class='badge'>").append(escapeHtml(cat)).append("</span></td>");
                html.append("<td>").append(emailCount).append("</td>");
                html.append("<td>").append(phoneCount).append("</td>");
                html.append("<td>");
                html.append("<a href='/showEmails?i=").append(i).append("'>Emails</a> · ");
                html.append("<a href='/showPhones?i=").append(i).append("'>Numbers</a>");
                html.append("</td></tr>");
            }
            html.append("</tbody></table>");
        }
        html.append("</div>");

        // categories card (EMAILS ONLY)
        html.append("<div class='card'>");
        html.append("<h2>By category</h2>");
        html.append("<p class='tagline'>Email reach per lane. Numbers are handled separately by show if needed.</p>");
        html.append("<table><thead><tr>");
        html.append("<th>Category</th><th>Emails (unique)</th><th>Email list</th>");
        html.append("</tr></thead><tbody>");
        addCategoryRow(html, "Afrosoul", data.catAfroEmails, data.catAfroPhones);
        addCategoryRow(html, "Jazz",     data.catJazzEmails, data.catJazzPhones);
        addCategoryRow(html, "Comedy",   data.catComedyEmails, data.catComedyPhones);
        addCategoryRow(html, "Folk",     data.catPoetryEmails, data.catPoetryPhones);
        addCategoryRow(html, "HipHop",   data.catHipHopEmails, data.catHipHopPhones);
        addCategoryRow(html, "Reggae",   data.catReggaeEmails, data.catReggaePhones);
        html.append("</tbody></table>");

        html.append("<h3>All contacts</h3>");
        html.append("<p class='muted'>Total unique contacts: ").append(data.contactEmails.size())
                .append("<br>Full export (Email, Number, ShowsAttended) is kept in <code>contacts_with_shows.csv</code> in this folder.</p>");
        html.append("</div>"); // end categories card

        html.append("</div>"); // end grid

        // DATA INSIGHTS SECTION BUTTON
        html.append("<div class='card' style='margin-top:20px;'>");
        html.append("<h2>Data insights section</h2>");
        html.append("<p class='tagline'>View deeper analytics about buyers, repeat audiences and category behaviour.</p>");
        html.append("<p class='muted'>Includes retention rates, cross-category fans, ticket volume per lane and group buying patterns.</p>");
        html.append("<button type='button' onclick=\"location.href='/dataInsights'\">Open data insights</button>");
        html.append("</div>");

        // tools
        html.append("<div class='card' style='margin-top:20px;'>");
        html.append("<h2>Tools</h2>");

        html.append("<h3>Upload new CSV</h3>");
        html.append("<form method='post' action='/upload' enctype='multipart/form-data'>");
        html.append("<input type='file' name='csvFile' accept='.csv' required> ");
        html.append("<button type='submit'>Upload</button>");
        html.append("</form>");
        html.append("<p class='muted'>Uploaded CSVs are saved in this folder and automatically included on refresh.</p>");

        html.append("<h3>Data settings</h3>");
        html.append("<p><a href='/exclusions'>Manage excluded emails</a> · ");
        html.append("<a href='/categories'>Edit show categories</a></p>");

        html.append("</div>");

        html.append("<div class='footer'>The One Room Music & Comedy Club · The One Room Database Manager (Java).</div>");
        html.append("</div></body></html>");

        sendHtml(ex, html.toString());
    }



    // ===================== SHOW EMAILS ======================

    private static void handleShowEmails(HttpExchange ex) throws IOException {
        ProcessedData data = processFolder(new File("."));
        int idx = parseIntQueryParam(ex.getRequestURI().getQuery(), "i", -1);

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Show emails</title>");
        html.append(sharedCss());
        html.append("</head><body><div class='wrapper'>");

        if (idx < 0 || idx >= data.showNames.size()) {
            html.append("<div class='card'><h2>Error</h2><p class='muted'>Invalid show index.</p>");
            html.append("<p><a href='/'>Back to main dashboard</a></p></div></div></body></html>");
            sendHtml(ex, html.toString());
            return;
        }

        String show = data.showNames.get(idx);
        List<String> emails = data.showEmailsLists.get(idx);

        html.append("<div class='hero'>");
        html.append("<div class='hero-pill'>Show emails</div>");
        html.append("<h1 class='hero-title'>").append(escapeHtml(show)).append("</h1>");
        html.append("<p class='hero-sub'>Unique email addresses for this show.</p>");
        html.append("<p class='hero-meta'>Total emails: ").append(emails.size()).append("</p>");
        html.append("</div>");

        html.append("<div class='card'><h2>Email list</h2>");
        html.append("<table><thead><tr><th>#</th><th>Email</th></tr></thead><tbody>");
        for (int i = 0; i < emails.size(); i++) {
            html.append("<tr><td>").append(i + 1).append("</td><td>")
                    .append(escapeHtml(emails.get(i))).append("</td></tr>");
        }
        html.append("</tbody></table>");

        html.append("<h3>CSV preview (Email)</h3>");
        html.append("<pre style='font-size:12px;'>Email\n");
        for (String e : emails) {
            html.append(escapeCsv(e)).append("\n");
        }
        html.append("</pre>");
        html.append("<p><a href='/download?kind=showEmails&i=").append(idx)
                .append("'>Download CSV</a></p>");

        html.append("<p><a href='/'>Back to main dashboard</a></p></div></div></body></html>");

        sendHtml(ex, html.toString());
    }

    // ===================== SHOW NUMBERS ======================

    private static void handleShowPhones(HttpExchange ex) throws IOException {
        ProcessedData data = processFolder(new File("."));
        int idx = parseIntQueryParam(ex.getRequestURI().getQuery(), "i", -1);

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Show numbers</title>");
        html.append(sharedCss());
        html.append("</head><body><div class='wrapper'>");

        if (idx < 0 || idx >= data.showNames.size()) {
            html.append("<div class='card'><h2>Error</h2><p class='muted'>Invalid show index.</p>");
            html.append("<p><a href='/'>Back to main dashboard</a></p></div></div></body></html>");
            sendHtml(ex, html.toString());
            return;
        }

        String show = data.showNames.get(idx);
        List<String> phones = uniqueNonEmptyPhones(data.showPhoneLists.get(idx));

        html.append("<div class='hero'>");
        html.append("<div class='hero-pill'>Show numbers</div>");
        html.append("<h1 class='hero-title'>").append(escapeHtml(show)).append("</h1>");
        html.append("<p class='hero-sub'>Unique numbers for this show. Each contact name is Show + running number.</p>");
        html.append("<p class='hero-meta'>Total unique numbers: ").append(phones.size()).append("</p>");
        html.append("</div>");

        html.append("<div class='card'><h2>Numbers</h2>");
        html.append("<table><thead><tr><th>#</th><th>Name</th><th> Phone number</th></tr></thead><tbody>");
        int counter = 1;
        for (String p : phones) {
            String numStr = (counter < 10 ? "0" + counter : String.valueOf(counter));
            String name   = show + " " + numStr;
            html.append("<tr><td>").append(counter).append("</td><td>")
                    .append(escapeHtml(name)).append("</td><td>")
                    .append(escapeHtml(p)).append("</td></tr>");
            counter++;
        }
        html.append("</tbody></table>");

        html.append("<h3>CSV preview (Name,Number)</h3>");
        html.append("<pre style='font-size:12px;'>Name,Phone number\n");
        counter = 1;
        for (String p : phones) {
            String numStr = (counter < 10 ? "0" + counter : String.valueOf(counter));
            String name   = show + " " + numStr;
            html.append(escapeCsv(name)).append(",").append(escapeCsv(p)).append("\n");
            counter++;
        }
        html.append("</pre>");
        html.append("<p><a href='/download?kind=showPhones&i=").append(idx)
                .append("'>Download CSV</a></p>");

        html.append("<p><a href='/'>Back to main dashboard</a></p></div></div></body></html>");

        sendHtml(ex, html.toString());
    }

    // ===================== CATEGORY EMAILS VIEW ======================

    private static void handleCategoryContacts(HttpExchange ex) throws IOException {
        File folder = new File(".");
        ProcessedData data = processFolder(folder);
        String catParam = getQueryParam(ex.getRequestURI().getQuery(), "cat");
        String cat = normalizeCategory(catParam);
        if (cat == null) {
            sendHtml(ex, "<html><body><p>Invalid category.</p><p><a href='/'>Back</a></p></body></html>");
            return;
        }

        List<String> overrideShows = new ArrayList<>();
        List<String> overrideCats  = new ArrayList<>();
        loadCategoryOverrides(folder, overrideShows, overrideCats);

        List<String> emails = new ArrayList<>();
        List<String> showsJoined = new ArrayList<>();

        for (int i = 0; i < data.contactEmails.size(); i++) {
            List<String> contactShows = data.contactShows.get(i);
            List<String> inCat = new ArrayList<>();
            for (String s : contactShows) {
                String c = categoryForShow(s, overrideShows, overrideCats);
                if (c.equalsIgnoreCase(cat)) inCat.add(s);
            }
            if (!inCat.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < inCat.size(); j++) {
                    if (j > 0) sb.append(" | ");
                    sb.append(inCat.get(j));
                }
                emails.add(data.contactEmails.get(i) == null ? "" : data.contactEmails.get(i));
                showsJoined.add(sb.toString());
            }
        }

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Category emails</title>");
        html.append(sharedCss());
        html.append("</head><body><div class='wrapper'>");

        html.append("<div class='hero'>");
        html.append("<div class='hero-pill'>Category emails</div>");
        html.append("<h1 class='hero-title'>").append(escapeHtml(cat)).append("</h1>");
        html.append("<p class='hero-sub'>All contacts for this category with their emails and attended shows.</p>");
        html.append("<p class='hero-meta'>Total contacts (by email): ").append(emails.size()).append("</p>");
        html.append("</div>");

        html.append("<div class='card'><h2>Emails</h2>");
        html.append("<table><thead><tr><th>#</th><th>Email</th><th>Shows attended</th></tr></thead><tbody>");
        for (int i = 0; i < emails.size(); i++) {
            html.append("<tr><td>").append(i + 1).append("</td><td>")
                    .append(escapeHtml(emails.get(i))).append("</td><td>")
                    .append(escapeHtml(showsJoined.get(i))).append("</td></tr>");
        }
        html.append("</tbody></table>");

        html.append("<h3>CSV preview (Email)</h3>");
        html.append("<pre style='font-size:12px;'>Email\n");
        for (String em : emails) {
            html.append(escapeCsv(em)).append("\n");
        }
        html.append("</pre>");
        html.append("<p><a href='/download?kind=categoryContacts&cat=").append(cat)
                .append("'>Download CSV</a></p>");

        html.append("<p><a href='/'>Back to main dashboard</a></p></div></div></body></html>");
        sendHtml(ex, html.toString());
    }

    // ===================== CATEGORY NUMBERS ======================

    private static void handleCategoryPhones(HttpExchange ex) throws IOException {
        ProcessedData data = processFolder(new File("."));
        String catParam = getQueryParam(ex.getRequestURI().getQuery(), "cat");
        String cat = normalizeCategory(catParam);
        if (cat == null) {
            sendHtml(ex, "<html><body><p>Invalid category.</p><p><a href='/'>Back</a></p></body></html>");
            return;
        }

        List<String> phonesRaw;
        if (cat.equals("Afrosoul")) phonesRaw = data.catAfroPhones;
        else if (cat.equals("Jazz")) phonesRaw = data.catJazzPhones;
        else if (cat.equals("Comedy")) phonesRaw = data.catComedyPhones;
        else if (cat.equals("Folk")) phonesRaw = data.catPoetryPhones;
        else if (cat.equals("HipHop")) phonesRaw = data.catHipHopPhones;
        else phonesRaw = data.catReggaePhones;

        List<String> phones = uniqueNonEmptyPhones(phonesRaw);

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Category numbers</title>");
        html.append(sharedCss());
        html.append("</head><body><div class='wrapper'>");

        html.append("<div class='hero'>");
        html.append("<div class='hero-pill'>Category numbers</div>");
        html.append("<h1 class='hero-title'>").append(escapeHtml(cat)).append("</h1>");
        html.append("<p class='hero-sub'>All unique numbers for this category.</p>");
        html.append("<p class='hero-meta'>Total unique numbers: ").append(phones.size()).append("</p>");
        html.append("</div>");

        html.append("<div class='card'><h2>Numbers</h2>");
        html.append("<table><thead><tr><th>#</th><th>Name</th><th>Phone number</th></tr></thead><tbody>");
        int counter = 1;
        for (String p : phones) {
            String numStr = (counter < 10 ? "0" + counter : String.valueOf(counter));
            String name   = cat + " " + numStr;
            html.append("<tr><td>").append(counter).append("</td><td>")
                    .append(escapeHtml(name)).append("</td><td>")
                    .append(escapeHtml(p)).append("</td></tr>");
            counter++;
        }
        html.append("</tbody></table>");

        html.append("<h3>CSV preview (Name,Number)</h3>");
        html.append("<pre style='font-size:12px;'>Name,Number\n");
        counter = 1;
        for (String p : phones) {
            String numStr = (counter < 10 ? "0" + counter : String.valueOf(counter));
            String name   = cat + " " + numStr;
            html.append(escapeCsv(name)).append(",").append(escapeCsv(p)).append("\n");
            counter++;
        }
        html.append("</pre>");
        html.append("<p><a href='/download?kind=categoryPhones&cat=").append(cat)
                .append("'>Download CSV</a></p>");

        html.append("<p><a href='/'>Back to main dashboard</a></p></div></div></body></html>");
        sendHtml(ex, html.toString());
    }

    // ===================== LOGO ======================

    private static void handleLogo(HttpExchange ex) throws IOException {
        File folder = new File(".");
        File use = null;
        String mime = "image/png";

        File png = new File(folder, "one_room_logo.png");
        File jpg = new File(folder, "one_room_logo.jpg");
        if (png.exists()) { use = png; mime = "image/png"; }
        else if (jpg.exists()) { use = jpg; mime = "image/jpeg"; }
        else {
            File[] imgs = folder.listFiles((d, name) ->
                    name.toLowerCase().endsWith(".png") ||
                            name.toLowerCase().endsWith(".jpg") ||
                            name.toLowerCase().endsWith(".jpeg"));
            if (imgs != null && imgs.length > 0) {
                use = imgs[0];
                String n = use.getName().toLowerCase();
                mime = n.endsWith(".png") ? "image/png" : "image/jpeg";
            }
        }

        if (use == null) {
            ex.sendResponseHeaders(404, 0);
            ex.getResponseBody().close();
            return;
        }

        byte[] bytes;
        try (InputStream in = new FileInputStream(use)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buf = new byte[4096]; int r;
            while ((r = in.read(buf)) != -1) baos.write(buf, 0, r);
            bytes = baos.toByteArray();
        }

        ex.getResponseHeaders().set("Content-Type", mime);
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    // ===================== DOWNLOAD CSV ======================

    private static void handleDownload(HttpExchange ex) throws IOException {
        File folder = new File(".");
        ProcessedData data = processFolder(folder);
        String query = ex.getRequestURI().getQuery();
        String kind  = getQueryParam(query, "kind");
        if (kind == null) kind = "";

        String filename = "export.csv";
        StringBuilder csv = new StringBuilder();

        if (kind.equals("showEmails")) {
            int idx = parseIntQueryParam(query, "i", -1);
            if (idx < 0 || idx >= data.showNames.size()) { send404(ex); return; }
            List<String> emails = data.showEmailsLists.get(idx);
            filename = sanitizeFileName(data.showNames.get(idx) + "_emails.csv");
            csv.append("Email\n");
            for (String e : emails) csv.append(escapeCsv(e)).append("\n");

        } else if (kind.equals("showPhones")) {
            int idx = parseIntQueryParam(query, "i", -1);
            if (idx < 0 || idx >= data.showNames.size()) { send404(ex); return; }
            String show = data.showNames.get(idx);
            List<String> phones = uniqueNonEmptyPhones(data.showPhoneLists.get(idx));
            filename = sanitizeFileName(show + "_numbers.csv");
            csv.append("Name,Number\n");
            int counter = 1;
            for (String p : phones) {
                String numStr = (counter < 10 ? "0" + counter : String.valueOf(counter));
                String name   = show + " " + numStr;
                csv.append(escapeCsv(name)).append(",").append(escapeCsv(p)).append("\n");
                counter++;
            }

        } else if (kind.equals("categoryContacts")) {
            String catParam = getQueryParam(query, "cat");
            String cat = normalizeCategory(catParam);
            if (cat == null) { send404(ex); return; }
            filename = sanitizeFileName(cat + "_emails.csv");

            List<String> overrideShows = new ArrayList<>();
            List<String> overrideCats  = new ArrayList<>();
            loadCategoryOverrides(folder, overrideShows, overrideCats);

            csv.append("Email\n");
            for (int i = 0; i < data.contactEmails.size(); i++) {
                List<String> contactShows = data.contactShows.get(i);
                boolean inThisCat = false;
                for (String s : contactShows) {
                    String c = categoryForShow(s, overrideShows, overrideCats);
                    if (c.equalsIgnoreCase(cat)) { inThisCat = true; break; }
                }
                if (inThisCat) {
                    csv.append(escapeCsv(data.contactEmails.get(i))).append("\n");
                }
            }

        } else if (kind.equals("categoryPhones")) {
            String catParam = getQueryParam(query, "cat");
            String cat = normalizeCategory(catParam);
            if (cat == null) { send404(ex); return; }
            filename = sanitizeFileName(cat + "_numbers.csv");

            List<String> phonesRaw;
            if (cat.equals("Afrosoul")) phonesRaw = data.catAfroPhones;
            else if (cat.equals("Jazz")) phonesRaw = data.catJazzPhones;
            else if (cat.equals("Comedy")) phonesRaw = data.catComedyPhones;
            else if (cat.equals("Folk")) phonesRaw = data.catPoetryPhones;
            else if (cat.equals("HipHop")) phonesRaw = data.catHipHopPhones;
            else phonesRaw = data.catReggaePhones;

            List<String> phones = uniqueNonEmptyPhones(phonesRaw);
            csv.append("Name,Number\n");
            int counter = 1;
            for (String p : phones) {
                String numStr = (counter < 10 ? "0" + counter : String.valueOf(counter));
                String name   = cat + " " + numStr;
                csv.append(escapeCsv(name)).append(",").append(escapeCsv(p)).append("\n");
                counter++;
            }
        } else {
            send404(ex);
            return;
        }

        byte[] bytes = csv.toString().getBytes(StandardCharsets.UTF_8);
        Headers h = ex.getResponseHeaders();
        h.set("Content-Type", "text/csv; charset=UTF-8");
        h.set("Content-Disposition", "attachment; filename=\"" + filename + "\"");
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private static String sanitizeFileName(String s) {
        if (s == null) return "export.csv";
        return s.replaceAll("[^a-zA-Z0-9_\\-\\.]", "_");
    }

    private static void send404(HttpExchange ex) throws IOException {
        sendHtml(ex, "<html><body><p>Not found.</p><p><a href='/'>Back</a></p></body></html>");
    }

    private static String normalizeCategory(String cat) {
        if (cat == null) return null;
        String l = cat.toLowerCase();
        if (l.equals("afrosoul")) return "Afrosoul";
        if (l.equals("jazz"))     return "Jazz";
        if (l.equals("comedy"))   return "Comedy";
        if (l.equals("poetry") || l.equals("folk"))   return "Folk";
        if (l.equals("hiphop"))   return "HipHop";
        if (l.equals("reggae"))   return "Reggae";
        return null;
    }

    // ===================== UPLOAD ======================

    private static void handleUpload(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            ex.getResponseHeaders().set("Location", "/");
            ex.sendResponseHeaders(302, -1);
            ex.close();
            return;
        }

        Headers headers = ex.getRequestHeaders();
        String ct = headers.getFirst("Content-Type");
        if (ct == null || !ct.contains("multipart/form-data")) {
            sendHtml(ex, "<html><body><p>Invalid upload.</p><p><a href='/'>Back</a></p></body></html>");
            return;
        }

        String boundary = null;
        for (String part : ct.split(";")) {
            part = part.trim();
            if (part.startsWith("boundary=")) {
                boundary = part.substring("boundary=".length());
                if (boundary.startsWith("\"") && boundary.endsWith("\"")) {
                    boundary = boundary.substring(1, boundary.length() - 1);
                }
            }
        }
        if (boundary == null) {
            sendHtml(ex, "<html><body><p>Upload error (boundary).</p><p><a href='/'>Back</a></p></body></html>");
            return;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (InputStream in = ex.getRequestBody()) {
            byte[] buf = new byte[4096]; int r;
            while ((r = in.read(buf)) != -1) baos.write(buf, 0, r);
        }
        String body = new String(baos.toByteArray(), StandardCharsets.ISO_8859_1);
        String fullBoundary = "--" + boundary;

        for (String part : body.split(fullBoundary)) {
            if (part.trim().isEmpty() || part.equals("--") || part.equals("--\r\n")) continue;
            int headerEnd = part.indexOf("\r\n\r\n");
            if (headerEnd < 0) continue;
            String headersBlock = part.substring(0, headerEnd);
            String dataBlock    = part.substring(headerEnd + 4);
            int endIdx = dataBlock.lastIndexOf("\r\n");
            if (endIdx >= 0) dataBlock = dataBlock.substring(0, endIdx);

            String dispLine = null;
            for (String hl : headersBlock.split("\r\n")) {
                if (hl.toLowerCase().startsWith("content-disposition")) dispLine = hl;
            }
            if (dispLine == null || !dispLine.contains("name=\"csvFile\"")) continue;

            String fileName = "uploaded_" + System.currentTimeMillis() + ".csv";
            int fnIdx = dispLine.toLowerCase().indexOf("filename=");
            if (fnIdx >= 0) {
                String fn = dispLine.substring(fnIdx + 9).trim();
                if (fn.startsWith("\"") && fn.endsWith("\"")) fn = fn.substring(1, fn.length() - 1);
                if (!fn.isEmpty()) fileName = fn;
            }
            if (!fileName.toLowerCase().endsWith(".csv")) fileName += ".csv";

            try (Writer w = new OutputStreamWriter(new FileOutputStream(new File(".", fileName)), StandardCharsets.UTF_8)) {
                w.write(dataBlock);
            }
            break;
        }

        sendHtml(ex, "<html><body><p>Upload complete.</p><p><a href='/'>Back to dashboard</a></p></body></html>");
    }

    // ===================== EXCLUSIONS ======================

    private static void handleExclusions(HttpExchange ex) throws IOException {
        File folder = new File(".");
        if ("POST".equalsIgnoreCase(ex.getRequestMethod())) {
            String body = readBody(ex);
            String text = getFormParam(body, "emailsText");
            if (text == null) text = "";

            List<String> lines = new ArrayList<>();
            BufferedReader br = new BufferedReader(new StringReader(text));
            String line;
            while ((line = br.readLine()) != null) {
                String em = line.trim().toLowerCase();
                if (!em.isEmpty() && !containsString(lines, em)) lines.add(em);
            }

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(folder, "excluded_emails.txt")))) {
                for (String em : lines) {
                    bw.write(em); bw.newLine();
                }
            }
        }

        List<String> excluded = loadExcludedEmails(folder);

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Excluded emails</title>");
        html.append(sharedCss());
        html.append("</head><body><div class='wrapper'>");
        html.append("<div class='hero'><div class='hero-pill'>Data settings</div>");
        html.append("<h1 class='hero-title'>Excluded emails</h1>");
        html.append("<p class='hero-sub'>Emails listed here will be ignored when building any list.</p>");
        html.append("<p class='hero-meta'>Current excluded emails: ").append(excluded.size()).append("</p></div>");

        html.append("<div class='card'><h2>Edit list</h2>");
        html.append("<form method='post' action='/exclusions'>");
        html.append("<p class='tagline'>One email per line. Saved in <code>excluded_emails.txt</code>.</p>");
        html.append("<textarea name='emailsText'>");
        for (String em : excluded) html.append(escapeHtml(em)).append("\n");
        html.append("</textarea><br><br><button type='submit'>Save</button></form>");
        html.append("<p><a href='/'>Back to main dashboard</a></p></div></div></body></html>");

        sendHtml(ex, html.toString());
    }

    // ===================== CATEGORY OVERRIDES EDITOR ======================

    private static void handleCategories(HttpExchange ex) throws IOException {
        File folder = new File(".");
        if ("POST".equalsIgnoreCase(ex.getRequestMethod())) {
            String body = readBody(ex);
            int total = 0;
            try {
                String t = getFormParam(body, "totalShows");
                if (t != null) total = Integer.parseInt(t);
            } catch (Exception ignored) {}

            List<String> outShows = new ArrayList<>();
            List<String> outCats  = new ArrayList<>();
            for (int i = 0; i < total; i++) {
                String show = getFormParam(body, "show_" + i);
                String cat  = getFormParam(body, "cat_"  + i);
                if (show == null) continue;
                show = show.trim();
                if (cat == null) cat = "";
                cat = cat.trim();
                if (!show.isEmpty() && !cat.isEmpty()) {
                    outShows.add(show);
                    outCats.add(cat);
                }
            }

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(folder, "categories.txt")))) {
                for (int i = 0; i < outShows.size(); i++) {
                    bw.write(escapeCsv(outShows.get(i))); bw.write(",");
                    bw.write(escapeCsv(outCats.get(i)));  bw.newLine();
                }
            }
        }

        ProcessedData data = processFolder(folder);
        List<String> overrideShows = new ArrayList<>();
        List<String> overrideCats  = new ArrayList<>();
        loadCategoryOverrides(folder, overrideShows, overrideCats);

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Edit categories</title>");
        html.append(sharedCss());
        html.append("</head><body><div class='wrapper'>");
        html.append("<div class='hero'><div class='hero-pill'>Data settings</div>");
        html.append("<h1 class='hero-title'>Show categories</h1>");
        html.append("<p class='hero-sub'>Override automatic guesses and set the category for each show.</p>");
        html.append("<p class='hero-meta'>Changes are stored in <code>categories.txt</code>.</p></div>");

        html.append("<div class='card'><h2>Assign categories</h2>");
        html.append("<form method='post' action='/categories'>");
        html.append("<input type='hidden' name='totalShows' value='").append(data.showNames.size()).append("'>");
        html.append("<table><thead><tr><th>#</th><th>Show</th><th>Current category</th><th>New category</th></tr></thead><tbody>");
        for (int i = 0; i < data.showNames.size(); i++) {
            String show = data.showNames.get(i);
            String current = categoryForShow(show, overrideShows, overrideCats);
            String guess   = categoryGuess(show);
            html.append("<tr><td>").append(i + 1).append("</td>");
            html.append("<td>").append(escapeHtml(show)).append("</td>");
            html.append("<td>").append(escapeHtml(current)).append("</td><td>");
            html.append("<input type='hidden' name='show_").append(i).append("' value='")
                    .append(escapeHtmlAttr(show)).append("'>");
            html.append("<select name='cat_").append(i).append("'>");
            html.append("<option value=''>Auto (").append(escapeHtml(guess)).append(")</option>");
            for (String c : CATEGORIES) {
                html.append("<option value='").append(escapeHtmlAttr(c)).append("'");
                if (current.equalsIgnoreCase(c)) html.append(" selected");
                html.append(">").append(escapeHtml(c)).append("</option>");
            }
            html.append("</select></td></tr>");
        }
        html.append("</tbody></table><br><button type='submit'>Save categories</button></form>");
        html.append("<p><a href='/'>Back to main dashboard</a></p></div></div></body></html>");

        sendHtml(ex, html.toString());
    }

    // ===================== PROCESSING LOGIC ======================

    private static ProcessedData processFolder(File folder) {
        ProcessedData data = new ProcessedData();

        data.showNames      = new ArrayList<>();
        data.showEmailsLists= new ArrayList<>();
        data.showPhoneLists = new ArrayList<>();

        data.catAfroEmails   = new ArrayList<>();
        data.catJazzEmails   = new ArrayList<>();
        data.catComedyEmails = new ArrayList<>();
        data.catPoetryEmails = new ArrayList<>();
        data.catHipHopEmails = new ArrayList<>();
        data.catReggaeEmails = new ArrayList<>();

        data.catAfroPhones   = new ArrayList<>();
        data.catJazzPhones   = new ArrayList<>();
        data.catComedyPhones = new ArrayList<>();
        data.catPoetryPhones = new ArrayList<>();
        data.catHipHopPhones = new ArrayList<>();
        data.catReggaePhones = new ArrayList<>();

        data.contactEmails = new ArrayList<>();
        data.contactPhones = new ArrayList<>();
        data.contactShows  = new ArrayList<>();

        data.showTicketCounts = new ArrayList<>();
        data.catTicketCounts  = new int[6];
        data.totalTickets     = 0;
        data.timeBuckets      = new int[4];
        data.groupSizeBuckets = new int[7];

        List<String> contactKeys = new ArrayList<>();

        List<String> excluded = loadExcludedEmails(folder);
        List<String> overrideShows = new ArrayList<>();
        List<String> overrideCats  = new ArrayList<>();
        loadCategoryOverrides(folder, overrideShows, overrideCats);

        // for group sizes (orders)
        List<String> orderKeys   = new ArrayList<>();
        List<Integer> orderCounts= new ArrayList<>();

        File[] csvs = folder.listFiles((d, n) ->
                n.toLowerCase().endsWith(".csv") &&
                        !n.equalsIgnoreCase("contacts_with_shows.csv") &&
                        !n.equalsIgnoreCase("all_contacts.csv"));
        if (csvs == null) csvs = new File[0];
        Arrays.sort(csvs, (a, b) -> a.getName().compareToIgnoreCase(b.getName()));

        for (File f : csvs) {
            String baseName = f.getName().substring(0, f.getName().length()-4);
            String catName  = categoryForShow(baseName, overrideShows, overrideCats);
            int catIdx      = categoryIndex(catName);

            data.showNames.add(baseName);
            List<String> showEmails = new ArrayList<>();
            List<String> showPhones = new ArrayList<>();
            data.showEmailsLists.add(showEmails);
            data.showPhoneLists.add(showPhones);
            data.showTicketCounts.add(0);
            int showIndex = data.showNames.size() - 1;

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(new FileInputStream(f), StandardCharsets.UTF_8))) {

                String header = br.readLine();
                if (header == null) continue;
                List<String> cols = parseCsvLine(header);
                int iEmail      = findIndex(cols, "Email");
                int iPurchEmail = findIndex(cols, "Purchaser Email");
                int iPhone      = findIndex(cols, "Cellphone");
                int iTicket     = findIndex(cols, "Ticket Number");
                int iOrder      = findIndex(cols, "Order Number");
                int iPayRef     = findIndex(cols, "Payment Reference");

                String line;
                while ((line = br.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    List<String> row = parseCsvLine(line);

                    String email = (iEmail >=0 && iEmail < row.size()) ? row.get(iEmail).trim().toLowerCase() : "";
                    String pEmail= (iPurchEmail>=0 && iPurchEmail< row.size()) ? row.get(iPurchEmail).trim().toLowerCase():"";
                    String phone = (iPhone >=0 && iPhone < row.size()) ? row.get(iPhone) : "";
                    if (phone == null) phone = "";
                    phone = phone.replaceAll("\\D", "");
                    if (phone.startsWith("27") && phone.length()==11) phone = "0"+phone.substring(2);
                    String ticket= (iTicket>=0 && iTicket<row.size()) ? row.get(iTicket).trim() : "";

                    boolean isExcluded = (!email.isEmpty()  && containsString(excluded, email)) ||
                            (!pEmail.isEmpty() && containsString(excluded, pEmail));
                    if (isExcluded) continue;

                    String finalEmail = !email.isEmpty() ? email : pEmail;

                    // contact key for deduping
                    String key = !email.isEmpty() ? "email:"+email
                            : !phone.isEmpty() ? "phone:"+phone
                            : !pEmail.isEmpty()? "email:"+pEmail
                            : "ticket:"+ticket;

                    int idxKey = indexOfString(contactKeys, key);
                    if (idxKey == -1) {
                        contactKeys.add(key);
                        data.contactEmails.add(finalEmail);
                        data.contactPhones.add(phone);
                        List<String> shows = new ArrayList<>();
                        shows.add(baseName);
                        data.contactShows.add(shows);
                    } else {
                        if (!finalEmail.isEmpty()) {
                            String existing = data.contactEmails.get(idxKey);
                            if (existing == null || existing.isEmpty())
                                data.contactEmails.set(idxKey, finalEmail);
                        }
                        if (!phone.isEmpty()) {
                            String existing = data.contactPhones.get(idxKey);
                            if (existing == null || existing.isEmpty())
                                data.contactPhones.set(idxKey, phone);
                        }
                        List<String> shows = data.contactShows.get(idxKey);
                        if (!containsString(shows, baseName)) shows.add(baseName);
                    }

                    // per-show emails/phones (no duplicates)
                    if (!finalEmail.isEmpty() && !containsString(showEmails, finalEmail))
                        showEmails.add(finalEmail);
                    if (!phone.isEmpty() && !containsString(showPhones, phone))
                        showPhones.add(phone);

                    // category aggregates (emails, phones)
                    if (!finalEmail.isEmpty()) {
                        switch (catIdx) {
                            case 1: if (!containsString(data.catAfroEmails, finalEmail))   data.catAfroEmails.add(finalEmail);   break;
                            case 2: if (!containsString(data.catJazzEmails, finalEmail))   data.catJazzEmails.add(finalEmail);   break;
                            case 3: if (!containsString(data.catComedyEmails, finalEmail)) data.catComedyEmails.add(finalEmail); break;
                            case 4: if (!containsString(data.catPoetryEmails, finalEmail)) data.catPoetryEmails.add(finalEmail); break; // Folk
                            case 5: if (!containsString(data.catHipHopEmails, finalEmail)) data.catHipHopEmails.add(finalEmail); break;
                            case 6: if (!containsString(data.catReggaeEmails, finalEmail)) data.catReggaeEmails.add(finalEmail); break;
                        }
                    }
                    if (!phone.isEmpty()) {
                        switch (catIdx) {
                            case 1: if (!containsString(data.catAfroPhones, phone))   data.catAfroPhones.add(phone);   break;
                            case 2: if (!containsString(data.catJazzPhones, phone))   data.catJazzPhones.add(phone);   break;
                            case 3: if (!containsString(data.catComedyPhones, phone)) data.catComedyPhones.add(phone); break;
                            case 4: if (!containsString(data.catPoetryPhones, phone)) data.catPoetryPhones.add(phone); break; // Folk
                            case 5: if (!containsString(data.catHipHopPhones, phone)) data.catHipHopPhones.add(phone); break;
                            case 6: if (!containsString(data.catReggaePhones, phone)) data.catReggaePhones.add(phone); break;
                        }
                    }

                    // ticket counts per show & category (financial proxy)
                    data.totalTickets++;
                    int currentShowCount = data.showTicketCounts.get(showIndex);
                    data.showTicketCounts.set(showIndex, currentShowCount + 1);
                    if (catIdx >= 1 && catIdx <= 6) {
                        data.catTicketCounts[catIdx - 1]++;
                    }

                    // group size via Payment Reference / Order Number
                    String orderKey = "";
                    if (iPayRef >= 0 && iPayRef < row.size()) {
                        String v = row.get(iPayRef);
                        if (v != null) orderKey = v.trim();
                    }
                    if (orderKey.isEmpty() && iOrder >= 0 && iOrder < row.size()) {
                        String v = row.get(iOrder);
                        if (v != null) orderKey = v.trim();
                    }
                    if (!orderKey.isEmpty()) {
                        int idxOrder = indexOfString(orderKeys, orderKey);
                        if (idxOrder == -1) {
                            orderKeys.add(orderKey);
                            orderCounts.add(1);
                        } else {
                            int c = orderCounts.get(idxOrder);
                            orderCounts.set(idxOrder, c + 1);
                        }
                    }

                    // purchase time-of-day detection from any date-time column
                    String timestamp = null;
                    for (String cell : row) {
                        if (cell == null) continue;
                        String v = cell.trim();
                        if (v.length() >= 13) {
                            String datePart = v.substring(0, 10);
                            if (datePart.matches("\\d{4}-\\d{2}-\\d{2}") && v.charAt(10) == ' ') {
                                timestamp = v;
                                break;
                            }
                        }
                    }
                    if (timestamp != null && timestamp.length() >= 13) {
                        try {
                            String hourStr = timestamp.substring(11, 13);
                            int hour = Integer.parseInt(hourStr);
                            if (hour >= 6 && hour <= 11) data.timeBuckets[0]++;       // morning
                            else if (hour >= 12 && hour <= 16) data.timeBuckets[1]++; // afternoon
                            else if (hour >= 17 && hour <= 21) data.timeBuckets[2]++; // evening
                            else data.timeBuckets[3]++;                                // late/other
                        } catch (Exception ignored) {}
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // group size buckets
        for (int c : orderCounts) {
            if (c <= 0) continue;
            if (c >= 6) data.groupSizeBuckets[6]++;
            else data.groupSizeBuckets[c]++;
        }

        // write contacts_with_shows
        writeContactsWithShows(new File(folder, "contacts_with_shows.csv"),
                data.contactEmails, data.contactPhones, data.contactShows);

        return data;
    }

    // ===================== SMALL HELPERS ======================

    private static boolean containsString(List<String> list, String key) {
        for (String s : list) if (s.equals(key)) return true;
        return false;
    }

    private static int indexOfString(List<String> list, String key) {
        for (int i = 0; i < list.size(); i++) if (list.get(i).equals(key)) return i;
        return -1;
    }

    private static int findIndex(List<String> headers, String name) {
        for (int i = 0; i < headers.size(); i++)
            if (headers.get(i).trim().equalsIgnoreCase(name)) return i;
        return -1;
    }

    private static List<String> parseCsvLine(String line) {
        List<String> out = new ArrayList<>();
        StringBuilder cell = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (inQuotes) {
                if (ch == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cell.append('"'); i++;
                    } else inQuotes = false;
                } else cell.append(ch);
            } else {
                if (ch == '"') inQuotes = true;
                else if (ch == ',') {
                    out.add(cell.toString()); cell.setLength(0);
                } else cell.append(ch);
            }
        }
        out.add(cell.toString());
        return out;
    }

    private static void writeContactsWithShows(File file,
                                               List<String> emails,
                                               List<String> phones,
                                               List<List<String>> shows) {
        try (BufferedWriter bw = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {
            bw.write("Email,Number,ShowsAttended\n");
            for (int i = 0; i < emails.size(); i++) {
                String email = escapeCsv(emails.get(i));
                String phone = escapeCsv(phones.get(i));
                List<String> sl = shows.get(i);
                StringBuilder sb = new StringBuilder();
                if (sl != null) {
                    for (int j = 0; j < sl.size(); j++) {
                        if (j > 0) sb.append(" | ");
                        sb.append(sl.get(j));
                    }
                }
                bw.write(email + "," + phone + "," + escapeCsv(sb.toString()) + "\n");
            }
        } catch (IOException e) { e.printStackTrace(); }
    }

    private static List<String> loadExcludedEmails(File folder) {
        List<String> list = new ArrayList<>();
        File f = new File(folder, "excluded_emails.txt");
        if (!f.exists()) return list;
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                String em = line.trim().toLowerCase();
                if (!em.isEmpty() && !containsString(list, em)) list.add(em);
            }
        } catch (IOException ignored) {}
        return list;
    }

    private static void loadCategoryOverrides(File folder, List<String> shows, List<String> cats) {
        File f = new File(folder, "categories.txt");
        if (!f.exists()) return;
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                List<String> cols = parseCsvLine(line);
                if (cols.size() < 2) continue;
                String show = cols.get(0).trim();
                String cat  = cols.get(1).trim();
                if (!show.isEmpty() && !cat.isEmpty()) {
                    shows.add(show); cats.add(cat);
                }
            }
        } catch (IOException ignored) {}
    }

    private static String categoryForShow(String baseName,
                                          List<String> overrideShows,
                                          List<String> overrideCats) {
        for (int i = 0; i < overrideShows.size(); i++)
            if (overrideShows.get(i).equalsIgnoreCase(baseName))
                return overrideCats.get(i);
        return categoryGuess(baseName);
    }

    private static List<String> uniqueNonEmptyPhones(List<String> src) {
        List<String> out = new ArrayList<>();
        for (String p : src) {
            if (p == null) continue;
            String v = p.trim();
            if (v.isEmpty()) continue;
            if (!containsString(out, v)) out.add(v);
        }
        return out;
    }

    private static String categoryGuess(String name) {
        String s = name.toLowerCase();
        if (s.contains("jazz") || s.contains("feya") || s.contains("tutu") || s.contains("pasha"))
            return "Jazz";
        if (s.contains("comedy") || s.contains("gumbi") || s.contains("robvan") || s.contains("rob van") || s.contains("tats"))
            return "Comedy";
        if (s.contains("poetry") || s.contains("poet") || s.contains("folk"))
            return "Folk";
        if (s.contains("hiphop") || s.contains("hip hop") || s.contains("urban") || s.contains("yahkeem") || s.contains("yakheem"))
            return "HipHop";
        if (s.contains("reggae") || s.contains("selassie") || s.contains("survivals") || s.contains("420") || s.contains("dub"))
            return "Reggae";
        if (s.contains("soul") || s.contains("afro") || s.contains("zamajobe") || s.contains("mxo") ||
                s.contains("camagwini") || s.contains("zolani") || s.contains("bongeziwe") || s.contains("ntsika") ||
                s.contains("asanda") || s.contains("brenda"))
            return "Afrosoul";
        return "Afrosoul";
    }

    private static int categoryIndex(String n) {
        String s = n.toLowerCase();
        if (s.equals("afrosoul")) return 1;
        if (s.equals("jazz"))     return 2;
        if (s.equals("comedy"))   return 3;
        if (s.equals("folk"))     return 4;
        if (s.equals("hiphop"))   return 5;
        if (s.equals("reggae"))   return 6;
        return 1;
    }

    private static void sendHtml(HttpExchange ex, String html) throws IOException {
        byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private static String escapeHtml(String s) {
        if (s == null) return "";
        return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;");
    }

    private static String escapeHtmlAttr(String s) {
        return escapeHtml(s).replace("\"","&quot;");
    }

    private static String escapeCsv(String v) {
        if (v == null) return "";
        if (v.contains(",") || v.contains("\"")) {
            v = "\"" + v.replace("\"","\"\"") + "\"";
        }
        return v;
    }

    private static int parseIntQueryParam(String query, String name, int def) {
        String val = getQueryParam(query, name);
        if (val == null) return def;
        try { return Integer.parseInt(val); } catch (Exception e) { return def; }
    }

    private static String getQueryParam(String query, String name) {
        if (query == null) return null;
        for (String p : query.split("&")) {
            int eq = p.indexOf('=');
            if (eq <= 0) continue;
            String k = p.substring(0, eq);
            String v = p.substring(eq + 1);
            if (k.equals(name)) {
                try { return URLDecoder.decode(v, "UTF-8"); }
                catch (Exception e) { return v; }
            }
        }
        return null;
    }

    private static String readBody(HttpExchange ex) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (InputStream in = ex.getRequestBody()) {
            byte[] buf = new byte[4096]; int r;
            while ((r = in.read(buf)) != -1) baos.write(buf, 0, r);
        }
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    private static String getFormParam(String body, String name) {
        if (body == null) return null;
        for (String p : body.split("&")) {
            int eq = p.indexOf('=');
            if (eq <= 0) continue;
            String k = p.substring(0, eq);
            String v = p.substring(eq + 1);
            if (k.equals(name)) {
                try { return URLDecoder.decode(v, "UTF-8"); }
                catch (Exception e) { return v; }
            }
        }
        return null;
    }

    // ===================== CSS HELPERS ======================

    private static String rootCss() {
        StringBuilder css = new StringBuilder();
        css.append("<style>");
        css.append("body{margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;");
        css.append("background:radial-gradient(circle at top,#2b1b33 0,#0b0b10 40%,#050509 100%);color:#f5f5f5;}");
        css.append(".wrapper{max-width:1100px;margin:0 auto;padding:24px 16px 40px;}");
        css.append(".hero{position:relative;padding:20px 24px 22px;margin-bottom:24px;border-radius:18px;");
        css.append("background:linear-gradient(135deg,rgba(255,204,128,0.06),rgba(255,255,255,0.02));");
        css.append("border:1px solid rgba(255,255,255,0.06);box-shadow:0 18px 45px rgba(0,0,0,0.75);overflow:hidden;}");
        css.append(".hero::before{content:'';position:absolute;right:-80px;top:-80px;width:220px;height:220px;");
        css.append("background:radial-gradient(circle,#f2c46b 0,rgba(242,196,107,0) 55%);opacity:0.7;}");
        css.append(".hero-inner{display:flex;align-items:center;gap:16px;position:relative;z-index:1;}");
        css.append(".logo-wrap{flex-shrink:0;width:88px;height:88px;border-radius:18px;background:rgba(0,0,0,0.55);");
        css.append("border:1px solid rgba(255,255,255,0.12);display:flex;align-items:center;justify-content:center;overflow:hidden;}");
        css.append(".logo-wrap img{max-width:80px;max-height:80px;display:block;}");
        css.append(".hero-text{flex:1;}");
        css.append(".hero-title{font-size:26px;font-weight:600;margin:0 0 4px;letter-spacing:0.03em;text-transform:uppercase;}");
        css.append(".hero-sub{margin:0 0 4px;font-size:14px;color:rgba(255,255,255,0.8);}");
        css.append(".hero-meta{font-size:12px;color:rgba(255,255,255,0.55);}");
        css.append(".hero-pill{display:inline-block;padding:4px 10px;margin-bottom:8px;border-radius:999px;");
        css.append("background:rgba(0,0,0,0.55);border:1px solid rgba(255,255,255,0.12);font-size:11px;");
        css.append("letter-spacing:0.16em;text-transform:uppercase;color:#f8e7c3;}");
        css.append(".grid{display:grid;grid-template-columns:2fr 1.2fr;grid-gap:20px;}");
        css.append(".card{border-radius:16px;background:rgba(8,8,15,0.9);border:1px solid rgba(255,255,255,0.06);");
        css.append("box-shadow:0 12px 30px rgba(0,0,0,0.7);padding:16px 18px 18px;}");
        css.append(".card h2{margin:0 0 10px;font-size:18px;font-weight:600;color:#f8e7c3;}");
        css.append(".card h3{margin:16px 0 8px;font-size:15px;font-weight:500;color:#f8e7c3;}");
        css.append(".tagline{font-size:13px;color:rgba(255,255,255,0.7);margin-bottom:8px;}");
        css.append("table{width:100%;border-collapse:collapse;font-size:13px;margin-top:6px;}");
        css.append("th,td{padding:6px 8px;border-bottom:1px solid rgba(255,255,255,0.06);text-align:left;}");
        css.append("th{font-size:12px;text-transform:uppercase;letter-spacing:0.08em;color:rgba(248,231,195,0.85);");
        css.append("background:rgba(255,255,255,0.03);}");
        css.append("tbody tr:hover{background:rgba(255,255,255,0.03);}");
        css.append("a{color:#f2c46b;text-decoration:none;}a:hover{color:#ffe3a5;text-decoration:underline;}");
        css.append(".badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:10px;");
        css.append("background:rgba(242,196,107,0.12);border:1px solid rgba(242,196,107,0.6);}");
        css.append(".muted{color:rgba(255,255,255,0.55);font-size:12px;}");
        css.append(".footer{margin-top:26px;font-size:11px;color:rgba(255,255,255,0.45);text-align:center;}");
        css.append("button{background:#f2c46b;color:#111;border:none;border-radius:999px;padding:6px 14px;");
        css.append("font-size:12px;cursor:pointer;}button:hover{background:#ffe3a5;}");
        css.append("input[type='file']{font-size:12px;color:#ddd;}");
        css.append("textarea{width:100%;min-height:180px;border-radius:10px;border:1px solid rgba(255,255,255,0.2);");
        css.append("background:rgba(0,0,0,0.6);color:#f5f5f5;font-size:12px;padding:8px;resize:vertical;}");
        css.append("@media(max-width:820px){.grid{grid-template-columns:1fr;}.hero-inner{flex-direction:column;align-items:flex-start;}}");
        css.append("</style>");
        return css.toString();
    }

    private static String sharedCss() {
        StringBuilder css = new StringBuilder();
        css.append("<style>");
        css.append("body{margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;");
        css.append("background:radial-gradient(circle at top,#2b1b33 0,#0b0b10 40%,#050509 100%);color:#f5f5f5;}");
        css.append(".wrapper{max-width:900px;margin:0 auto;padding:24px 16px 40px;}");
        css.append(".hero{position:relative;padding:24px 20px;margin-bottom:18px;border-radius:18px;");
        css.append("background:linear-gradient(135deg,rgba(255,204,128,0.06),rgba(255,255,255,0.02));");
        css.append("border:1px solid rgba(255,255,255,0.06);box-shadow:0 18px 45px rgba(0,0,0,0.75);overflow:hidden;}");
        css.append(".hero::before{content:'';position:absolute;right:-80px;top:-80px;width:220px;height:220px;");
        css.append("background:radial-gradient(circle,#f2c46b 0,rgba(242,196,107,0) 55%);opacity:0.7;}");
        css.append(".hero-title{font-size:24px;font-weight:600;margin:0 0 8px;letter-spacing:0.03em;text-transform:uppercase;}");
        css.append(".hero-sub{margin:0 0 4px;font-size:14px;color:rgba(255,255,255,0.8);}");
        css.append(".hero-meta{font-size:12px;color:rgba(255,255,255,0.55);}");
        css.append(".hero-pill{display:inline-block;padding:4px 10px;margin-bottom:8px;border-radius:999px;");
        css.append("background:rgba(0,0,0,0.55);border:1px solid rgba(255,255,255,0.12);font-size:11px;");
        css.append("letter-spacing:0.16em;text-transform:uppercase;color:#f8e7c3;}");
        css.append(".card{border-radius:16px;background:rgba(8,8,15,0.9);border:1px solid rgba(255,255,255,0.06);");
        css.append("box-shadow:0 12px 30px rgba(0,0,0,0.7);padding:16px 18px 18px;margin-bottom:14px;}");
        css.append(".card h2{margin:0 0 10px;font-size:18px;font-weight:600;color:#f8e7c3;}");
        css.append(".tagline{font-size:13px;color:rgba(255,255,255,0.7);margin-bottom:8px;}");
        css.append("table{width:100%;border-collapse:collapse;font-size:13px;margin-top:6px;}");
        css.append("th,td{padding:6px 8px;border-bottom:1px solid rgba(255,255,255,0.06);text-align:left;}");
        css.append("th{font-size:12px;text-transform:uppercase;letter-spacing:0.08em;color:rgba(248,231,195,0.85);");
        css.append("background:rgba(255,255,255,0.03);}");
        css.append("tbody tr:hover{background:rgba(255,255,255,0.03);}");
        css.append("a{color:#f2c46b;text-decoration:none;}a:hover{color:#ffe3a5;text-decoration:underline;}");
        css.append(".muted{color:rgba(255,255,255,0.55);font-size:12px;}");
        css.append("button{background:#f2c46b;color:#111;border:none;border-radius:999px;padding:6px 14px;");
        css.append("font-size:12px;cursor:pointer;}button:hover{background:#ffe3a5;}");
        css.append("textarea{width:100%;min-height:180px;border-radius:10px;border:1px solid rgba(255,255,255,0.2);");
        css.append("background:rgba(0,0,0,0.6);color:#f5f5f5;font-size:12px;padding:8px;resize:vertical;}");
        css.append(".chart-text{font-size:11px;color:rgba(255,255,255,0.7);margin-top:6px;}");
        css.append(".insight-list{font-size:12px;color:rgba(255,255,255,0.78);padding-left:18px;margin:8px 0 6px;}");
        css.append(".insight-list li{margin-bottom:6px;}");
        css.append("</style>");
        return css.toString();
    }

    private static void addCategoryRow(StringBuilder html,
                                       String catName,
                                       List<String> emails,
                                       List<String> phonesRaw) {
        int emailCount = emails.size();
        html.append("<tr>");
        html.append("<td>").append(escapeHtml(catName)).append("</td>");
        html.append("<td>").append(emailCount).append("</td>");
        html.append("<td><a href='/categoryContacts?cat=").append(catName)
                .append("'>View emails</a></td>");
        html.append("</tr>");
    }

    private static int maxIndex(double[] arr) {
        int idx = 0;
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] > arr[idx]) idx = i;
        }
        return idx;
    }

    private static int maxIndex(int[] arr) {
        int idx = 0;
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] > arr[idx]) idx = i;
        }
        return idx;
    }

    private static String formatCurrency(double value) {
        NumberFormat nf = NumberFormat.getCurrencyInstance(new Locale("en", "ZA"));
        return nf.format(value);
    }
}
