"use strict";
const basic_stat_csv = "data/basic_statistics.csv";
const post_breakdown = "data/post_breakdown.csv";
const comments_breakdown = "data/comments_breakdown.csv";
const sentiment_breakdown = "data/sentiment_score.csv";
//table csv
const post_table_csv = "data/top_score_posts.csv";
const user_table_csv = "data/top_author.csv";

const day_diff = 7;
function updateBasicStats(data) {
    document.querySelector("#numberOfPosts").textContent = data["Number of Posts"];
    document.querySelector("#numberOfComments").textContent = data["Number of Comments"];
    document.querySelector("#numberOfAuthors").textContent = data["Number of Authors"];
}

d3.csv(basic_stat_csv).then(function(data) {
    updateBasicStats(data[0])
});

function plot_graph(container_name, data_path, data_col, title, colorScale, width = 1100, height = 450) {
    d3.csv(data_path).then(function(data) {
        const startDate = d3.timeParse("%Y-%m-%d")(d3.min(data, d=>d.date)); // Adjust as necessary
        const endDate = d3.timeDay.offset(startDate, day_diff);
        const filterData = data.filter(d => {
            const dDate = d3.timeParse("%Y-%m-%d")(d.date);
            return dDate >= startDate && dDate <= endDate;
        });
        const parseDate = d3.timeParse("%Y-%m-%d");
        filterData.sort((a, b) => d3.ascending(parseDate(a.date), parseDate(b.date)));

    

        const marginTop = 75;
        const marginRight = 20;
        const marginBottom = 30;
        const marginLeft = 50;
        // Create the positional scales.
    
        const x = d3.scaleTime()
          .domain(d3.extent(filterData, d => parseDate(d.date)))
          .range([marginLeft, width - marginRight]);
      
        const y = d3.scaleLinear()
          .domain([d3.min(filterData, d => parseFloat(d[data_col])), d3.max(filterData, d => parseFloat(d[data_col]))]).nice()
          .range([height - marginBottom, marginTop]);
        
        // const colorScale = d3.scaleOrdinal()
        //     .domain(filterData.map(d => d.subreddit)) 
        //     .range(d3.schemeTableau10);
        
        const timeFormat = d3.timeFormat("%Y-%m-%d");
        const xAxis = d3.axisBottom(x)
            .ticks(d3.timeDay)
            .tickFormat(timeFormat);
      
        // Create the SVG container.
        let svg = d3.select(container_name).append("svg")
            .attr("width", width)
            .attr("height", height)
            .attr("viewBox", [0, 0, width, height])
            .attr("style", "max-width: 100%; height: auto; overflow: visible; font: 10px sans-serif;");
        
        svg.append("text") 
            .attr("x", width / 2) 
            .attr("y", marginTop / 4) 
            .attr("text-anchor", "middle") 
            .style("font-size", "18px") 
            .style("font-weight", "bold")
            .style("fill", "#0C63BD")
            .style("font-family", "Segoe UI, Tahoma, Geneva, Verdana, sans-serif") 
            .text(title);
    
        svg.append("g")
            .attr("transform", `translate(0, ${height - marginBottom})`)
            .call(xAxis);
      
        svg.append("g")
            .attr("transform", `translate(${marginLeft},0)`)
            .call(d3.axisLeft(y))
            .call(g => g.select(".domain").remove())
            .call(g => g.append("text")
                .attr("x", -marginLeft)
                .attr("y", 10)
                .attr("fill", "currentColor")
                .attr("text-anchor", "start")
            );
      
        // Compute the points in pixel space as [x, y, z], where z is the name of the series.
        const points = filterData.map((d) => [x(parseDate(d.date)), y(d[data_col]), d.subreddit, d[data_col]]);

      
        // Group the points by series.
        const groups = d3.rollup(points, v => Object.assign(v, {z: v[0][2]}), d => d[2]);
      
        // Draw the lines.
        const line = d3.line();
    
        const path = svg.append("g")
            .attr("fill", "none")
            // .attr("stroke", "steelblue")
            .attr("stroke-width", 3.5)
            .attr("stroke-linejoin", "round")
            .attr("stroke-linecap", "round")
          .selectAll("path")
          .data(groups.values())
          .join("path")
            .style("mix-blend-mode", "multiply")
            .attr("stroke", d => colorScale(d[0][2])) // Set stroke color based on subreddit
            .attr("d", line);
        
        svg.selectAll(".point")
            .data(filterData)
            .enter().append("circle") 
            .attr("class", "point")
            .attr("cx", d => x(parseDate(d.date))) 
            .attr("cy", d => y(d[data_col])) 
            .attr("r", 3) 
            .attr("fill", d => colorScale(d.subreddit)) 
            .attr("stroke", "#fff") 
            .attr("stroke-width", 1); 
      
        // Add an invisible layer for the interactive tip.
        const dot = svg.append("g")
            .attr("display", "none");
      
        dot.append("circle")
            .attr("r", 4.5);
      
        dot.append("text")
            .attr("text-anchor", "middle")
            .attr("y", -8);
      
        svg
            .on("pointerenter", pointerentered)
            .on("pointermove", pointermoved)
            .on("pointerleave", pointerleft)
            .on("touchstart", event => event.preventDefault());
    
        function pointermoved(event) {
          const [xm, ym] = d3.pointer(event);
          const i = d3.leastIndex(points, ([x, y]) => Math.hypot(x - xm, y - ym));
          const [x, y, k, z] = points[i];
          path.style("stroke", ({z}) => z === k ? null : "#ddd").filter(({z}) => z === k).raise();
          dot.attr("transform", `translate(${x},${y})`);
          dot.select("text")
            .style("font-size", "15px")
            .text(`${k}, ${Math.round(z)}`);
          svg.property("value", filterData[i]).dispatch("input", {bubbles: true});
        }
      
        function pointerentered() {
          path.style("mix-blend-mode", null).style("stroke", "#ddd");
          dot.attr("display", null);
        }
      
        function pointerleft() {
          path.style("mix-blend-mode", "multiply").style("stroke", null);
          dot.attr("display", "none");
          svg.node().value = null;
          svg.dispatch("input", {bubbles: true});
        }
    
       
        const legendItemSpacing = 80
        const legendRectSize = 15; 
        const legendPosition = {x: marginLeft, y: marginTop - 35}; 
        
        const legend = svg.selectAll('.legend')
        .data(colorScale.domain())
        .enter()
        .append('g')
            .attr('class', 'legend')
            .attr('transform', (d, i) => {
            const xPosition = legendPosition.x + i * legendItemSpacing;
            return `translate(${xPosition}, ${legendPosition.y})`;
            });
        
        legend.append('rect')
        .attr('x', 0)
        .attr('y', 0)
        .attr('width', legendRectSize)
        .attr('height', legendRectSize)
        .style('fill', colorScale)
        .style('stroke', colorScale);
        
        legend.append('text')
        .attr('x', legendRectSize + 4)
        .attr('y', legendRectSize / 2)
        .attr('dy', '.35em')
        .style('text-anchor', 'start')
        .text(d => d);
        });
}

function populateTable(data, id) {
    const table = document.querySelector(id);
    //header row 
    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    Object.keys(data[0]).forEach(key => {
        const th = document.createElement('th');
        th.textContent = key;
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);
    //tobody
    const tbody = document.createElement('tbody');
    data.forEach(row => {
      const tr = document.createElement('tr');
      Object.values(row).forEach(text => {
        const td = document.createElement('td');
        // If the text looks like a URL, make it a clickable link
        if (text.startsWith('http')) {
          const a = document.createElement('a');
          a.href = text;
          a.textContent = text.includes('user') ? 'UserLink' : 'PostLink'; // Customize based on your needs
          a.target = '_blank'; // Open in new tab
          td.appendChild(a);
        } else {
          td.textContent = text.slice(0, 45);
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.append(tbody); 
  }
  

d3.csv(post_breakdown).then(data => {
    const colorScale = d3.scaleOrdinal()
        .domain(data.map(d=>d.subreddit))
        .range(d3.schemeTableau10);
    plot_graph(".page-left", post_breakdown, "n_posts", "Number of Posts", colorScale);
    plot_graph(".page-left", comments_breakdown, "n_comments", "Number of Comments", colorScale);

    d3.csv(post_table_csv).then(function(data) {
        // The data is now loaded and parsed
        populateTable(data, "#post-table");
    });
    d3.csv(user_table_csv).then(function(data) {
        // The data is now loaded and parsed
        populateTable(data, "#user-table");
    });
    plot_graph(".page-right", sentiment_breakdown, "Mean Sentiment Score", "Subreddit Sentiment Score", colorScale, 1000, 325);
});


