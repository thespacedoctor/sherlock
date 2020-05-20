

window.addEventListener('load', function() {

    var splitFootnotesAndCitations = function() {
        var originalContainer = document.body.getElementsByClassName('footnotes')
        if (originalContainer.length == 0) {
            return
        }

        var footnoteContainer = document.createElement('div')
        footnoteContainer.setAttribute('class', 'footnotes')
        var footnoteList = document.createElement('ol')
        footnoteContainer.appendChild(footnoteList)

        var citationContainer = document.createElement('div')
        citationContainer.setAttribute('class', 'citations')
        var citationList = document.createElement('ul')
        citationContainer.appendChild(citationList)

        var appendixContainer = document.createElement('div')
        appendixContainer.setAttribute('class', 'appendix')

        var destinationForItemClassName = function(className) {
            switch (className) {
                case 'citation':
                    console.log('citation')
                    return citationList
                case 'citation footnote-processed':
                    console.log('citation')
                    return citationList
                default:
                    console.log('footnote')
                    return footnoteList
            }
        }

        // SEPARATE CITATIONS AND FOOTNOTES.
        var originalListItems = originalContainer[0].getElementsByTagName('ol')[0].getElementsByTagName('li')
        for (var i = 0, l = originalListItems.length; i < l; i++) {
            var item = originalListItems[0]
            destinationForItemClassName(item.className).appendChild(item)
        }
        // DRAFTS
        var originalCitations = document.body.getElementsByClassName('citations')
        
        if (originalCitations.length) {
            originalCitations[0].id = "xghd"
            var originalCitationsItems = originalCitations[0].getElementsByTagName('ol')[0].getElementsByTagName('li')
            for (var i = 0, l = originalCitationsItems.length; i < l; i++) {
              var item = originalCitationsItems[0]
              citationList.appendChild(item)
              var elements = item.getElementsByTagName('a')
              elements[0].style.display = "none";
            }
        }

        // SORT CITATION CONTENT.
        var citationListItems = citationList.getElementsByTagName('li')
        var citationContent = []
        for (var i = 0, l = citationListItems.length; i < l; i++) {
            citationContent.push(citationListItems[i].innerHTML)
        }
        // citationContent.sort(function(a, b) {
        //     return a.toLowerCase().localeCompare(b.toLowerCase());
        // });
        // APPLY SORTED CONTENT TO CITATION ELEMENTS IN PLACE.
        for (var i = 0, l = citationListItems.length; i < l; i++) {
            citationListItems[i].innerHTML = citationContent[i]
        }

        // ADD FOOTNOTES PAGE(S) AND CITATION PAGE(S) TO APPENDIX
        var footnoteListItems = footnoteList.getElementsByTagName('li')

        if (footnoteListItems.length > 0) {
            footnoteList.insertAdjacentHTML('beforebegin', '<h1 style="page-break-before: always">Notes</h1>')
            appendixContainer.appendChild(footnoteContainer)
        }

        if (citationListItems.length > 0) {
            citationList.insertAdjacentHTML('beforebegin', '<h1 style="page-break-before: always">References</h1>')
            appendixContainer.appendChild(citationContainer)
        }

        originalContainer[0].parentNode.replaceChild(appendixContainer, originalContainer[0])
        originalCitations=document.getElementById("xghd");
        if (originalCitations != null) {
            originalCitations.parentNode.removeChild(originalCitations);
        }
        
    }

    // CONSOLIDATE FOOTNOTES
    var relabelFootnotes = function() {
        var footnoteMarkerlist = document.body.getElementsByClassName('footnote')
        if (!$(footnoteMarkerlist).hasClass('brackets')){
            for (var i = 0, l = footnoteMarkerlist.length; i < l; i++) {
                footnoteMarkerlist[i].innerHTML = i + 1
            }
        }

        footnoteButtons = document.body.getElementsByClassName('bigfoot-footnote__button')
        for (let item of footnoteButtons) {
            if (item.getAttribute("data-bigfoot-footnote").includes("citekey") || item.id.includes("cnref")) {
                item.setAttribute("data-footnote-number","c"+item.getAttribute("data-footnote-number"))
            } 
        }
    }

    // RENDER MATHJAX
    var processMath = function() {
        MathJax.Hub.Queue(["Typeset", MathJax.Hub]);
    }

    // MERMAID FLOWCHATS, GANTT AND SEQUENCE DIAGRAMS
    var mermaidCode = function() {
        

        mermaid.init(undefined, $("code.mermaid"));
        mermaid.init(undefined, $("div.highlight-mermaid div pre"));
       
    };

    // FLOWCHART RENDERING
    var flowchartCode = function myFunction() {
        var flowBlocks = document.querySelectorAll('code.flow');
        Array.prototype.forEach.call(flowBlocks, function(flowBlock, index) {

            // CREATE A UNIQUE ID FOR AN ELEMENT
            var randLetter = String.fromCharCode(65 + Math.floor(Math.random() * 26));
            var uniqid = randLetter + Date.now();

            // ADD DIV PLACEHOLDER TO ADD THE RENDERED FLOWCHART TO
            var diagram = document.createElement("div");
            diagram.id = uniqid
            document.body.insertBefore(diagram, flowBlock.parentNode);

            // GET CONTENT OF CODE BLOCK AND RENDER IT INTO THE ABOVE DIV
            var code = flowBlock.value || flowBlock.textContent;
            diagram = flowchart.parse(code);
            diagram.drawSVG(uniqid);

            // REMOVE ORIGINAL CODEBLOCK
            var del = flowBlock.parentElement;
            var parent = del.parentElement;
            parent.removeChild(del);
        })

        var parentDivs = document.querySelectorAll("div.highlight-flow");
        Array.prototype.forEach.call(parentDivs, function(parentDiv, index) {

            var flowBlock = parentDiv.querySelector("div>pre")

            // CREATE A UNIQUE ID FOR AN ELEMENT
            var randLetter = String.fromCharCode(65 + Math.floor(Math.random() * 26));
            var uniqid = randLetter + Date.now();

            // ADD DIV PLACEHOLDER TO ADD THE RENDERED FLOWCHART TO
            var diagram = document.createElement("div");
            diagram.id = uniqid
            parentDiv.insertBefore(diagram, flowBlock.parentNode);

            // GET CONTENT OF CODE BLOCK AND RENDER IT INTO THE ABOVE DIV
            var code = flowBlock.value || flowBlock.textContent;
            diagram = flowchart.parse(code);
            diagram.drawSVG(uniqid);

            // REMOVE ORIGINAL CODEBLOCK
            var del = flowBlock.parentElement;
            var parent = del.parentElement;
            parent.removeChild(del);
        })
    }

    // PRETTY SYNTAX HIGHLIGHTING FOR CODE BLOCKS
    var highlightCode = function() {

        // using highlight
        var mermaids = [];
        [].push.apply(mermaids, document.getElementsByClassName('mermaid'));
        for (i = 0; i < mermaids.length; i++) {
            mermaids[i].className = 'nohighlight mermaid';
        }
        var mermaids = [];
        [].push.apply(mermaids, document.getElementsByClassName('highlight-mermaid'));
        for (i = 0; i < mermaids.length; i++) {
            mermaids[i].className = 'nohighlight mermaid';
        }

        hljs.initHighlightingOnLoad();
        var blocks = $('pre > code')
        for (var i = 0; i < blocks.length; i++) {
            try {
                hljs.highlightBlock(blocks[i])
            } catch (error) {}
        }
    }

    // NICE FOOTNOTE POPOVERS   
    var bigfootCode = function() {
        // CITATIONS
        $.bigfoot({
            positionContent: true,
            preventPageScroll: false,
            actionOriginalFN: "ignore",
            anchorPattern: /(cn|fn|footnote|note):?\d+(-\d+)?citation/gi,
            numberResetSelector:"body"
        }); 
        // FOOTNOTES
        $.bigfoot({
            positionContent: true,
            preventPageScroll: false,
            actionOriginalFN: "ignore",
            anchorPattern: /(fn|footnote|note)\:?\d+(-\d+)?footnote/gi,
            numberResetSelector:"body"
        });
        // SPHINX FOOTNOTES + CITATION
        $.bigfoot({
            positionContent: true,
            preventPageScroll: false,
            actionOriginalFN: "ignore",
            anchorPattern: /fn:+/gi,
            numberResetSelector:"body"
        });
    };

    // const copyToClipboard = str => {
    //   const el = document.createElement('textarea');  // Create a <textarea> element
    //   el.value = str;                                 // Set its value to the string that you want copied
    //   el.setAttribute('readonly', '');                // Make it readonly to be tamper-proof
    //   el.style.position = 'absolute';                 
    //   el.style.left = '-9999px';                      // Move outside the screen to make it invisible
    //   document.body.appendChild(el);                  // Append the <textarea> element to the HTML document
    //   const selected =            
    //     document.getSelection().rangeCount > 0        // Check if there is any content selected previously
    //       ? document.getSelection().getRangeAt(0)     // Store selection if found
    //       : false;                                    // Mark as false to know no selection existed before
    //   el.select();                                    // Select the <textarea> content
    //   document.execCommand('copy');                   // Copy - only works as a result of a user action (e.g. click events)
    //   document.body.removeChild(el);                  // Remove the <textarea> element
    //   if (selected) {                                 // If a selection existed before copying
    //     document.getSelection().removeAllRanges();    // Unselect everything on the HTML document
    //     document.getSelection().addRange(selected);   // Restore the original selection
    // }
   
    // EXTRACT HASH-TAGS AND PRESENT THEM IN HTML RENDERING
    var processHashTags = function() {
        
        // GET HASH TAGS
        tagSpans = document.getElementsByClassName('hashtag')
        tags = []
        for (var i = 0; i < tagSpans.length; i++) {
            tags.push(tagSpans[i].innerHTML.replace("#", ""));
        }

        // METADATA IS IN PARAGRAPHS BEFORE FIRST HEADER
        var metadict = {}
        var c = document.body.childNodes;
        for (var i = 0; i < c.length; i++) {
            if (c[i].nodeName == "H1") {
                break
            }
            if (c[i].nodeName == "P" && c[i].innerHTML.includes(":")) {
                var dataelements = c[i].innerHTML.split("<br>")
                for (var j = 0; j < dataelements.length; j++) {
                    keyvalue = dataelements[j].trim().split(":")
                    metadict[keyvalue[0].trim()]=keyvalue[1].trim()
                }
                c[i].remove()
            }
        }

        // ADD LINKED HASH TAGS
        var tagsContainer = document.createElement('div')
        tagsContainer.setAttribute('class', 'tags')
        for (var i = 0; i < tags.length; i++) {
            if (i == 0) {tagsContainer.innerHTML="tags: "}
            tagsContainer.innerHTML += '<a href="ia-writer://quick-search?query=%23' + tags[i] + '" class="hashtag">#' + tags[i] + '</a>  '
        }

        // REMOVE VERY COMMON TAGS FROM SEARCH RESULTS
        var ignoreTags = ["cheatsheet", "★★★★★", "★★★★☆", "★★★☆☆", "★★☆☆☆", "★☆☆☆☆", "article", "tutorial", "podcast", "video"]
        for (var i = 0; i < ignoreTags.length; i++) {
            var index = tags.indexOf(ignoreTags[i]);
            if (index !== -1) tags.splice(index, 1);
        }

        return [tags, tagsContainer, metadict]
    }



    // ADD DEVONTHINK SEARCH
    var devonthinkLinks = function(tags) {
        var dtContainer = document.createElement('div')
        dtContainer.setAttribute('id', 'devonthinkLinks')
        
        // ADD DEVONTHINK SEARCHES - FIRST 'AND'ED THEN 'OR'ED
        var bm1 = "x-devonthink://search?query=%28"
        for (var i = 0; i < tags.length; i++) {
            bm1 += tags[i]
            if (i !== tags.length-1 ) {
                bm1 += "%20AND%20"
            }
        }
        bm1 += "%29%20OR%20"

        for (var i = 0; i < tags.length; i++) {
            bm1 += tags[i]
            if (i !== tags.length-1 ) {
                bm1 += "%20OR%20"
            }
        }
        
        var link = document.createElement('a')
        link.href = bm1
        link.text = "devonthink search"
        dtContainer.appendChild(link)
       
        return dtContainer

    }


    var pinboardLinks = function(tags) {
        // ADD PINBOARD HASH TAGS - FIRST ROUND IS TAGS COMBINED -- BEST MATCHES
        bookmarksElement =  document.createElement('div')
        bookmarksElement.setAttribute('id', 'pinboardLinks')

        var getLinks = function(data) {
            var show_bmarks = function(data) {
                var content = "";
                for (var i in data) {
                    var item = data[i];
                    var str = format_item(item);
                    content += str
                }
                return content
            }

            var format_item = function(it) {
                var str = "<div class=\"pin-item\">";
                if (!it.d) { return; }
                str += "<p><a class=\"pin-title\" href=\"" + cook(it.u) + "\">" + cook(it.d) + "</a><br/>";
                // if (it.n)
                // {
                //     str += "<span class=\"pin-description\">" + cook(it.n) + "</span><br/>\n";
                // }
                for (var i = 0; i < it.t.length; i++) 
                {
                    var tag = it.t[i];
                    if (!tag.includes("★")) {
                        str += " <a class=\"pin-tag\" href=\"https://pinboard.in/u:"+ cook(it.a) + "/t:" + cook(tag) + "\">#" + cook(tag) + "</a>  ";
                    } else {
                        str += " <a class=\"pin-tag\" href=\"https://pinboard.in/u:"+ cook(it.a) + "/t:" + cook(tag) + "\">" + cook(tag) + "</a>  ";
                    }
                    
                }
                
                str += "</div>\n";
                return str;
            }

            var cook = function(v) {
                if (v.replace)
                {
                    return v.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt>');
                }
                return '';
            }

            bookmarksElement.innerHTML+=show_bmarks(data);
        }


        var buildBookmarkList = function() {
            var bm = "https://feeds.pinboard.in/json/v1/secret:34d40014662bb9f918a8/u:thespacedoctor/"
            for (var i = 0; i < tags.length; i++) {
                    bm += "t:"+tags[i]+"/"
            }
            bm+="?count=5"
            $.getJSON(bm, getLinks);
            for (var i = 0; i < tags.length; i++) {
                var bm = "https://feeds.pinboard.in/json/v1/secret:34d40014662bb9f918a8/u:thespacedoctor/t:"+tags[i]+"/?count=3"
                $.getJSON(bm, getLinks);
            }    

            return bookmarksElement
        }

        return buildBookmarkList();
    };

    // CONVERT ADJACENT LINKS IA LINKS
    var convertLinksToIaLinks = function() {
        var anchors = document.getElementsByTagName('a');
        // be careful with variable names for items went compiling JS
        for (i = 0; i < anchors.length; i++) {
            var a = anchors[i];
            if (!a.href.includes("#") && a.href.includes("file://") && a.href.includes("Contents/Resources/")) {
                a.href = a.href.replace(/file.*?Resources\//g, "ia-writer://open?path=/").replace("&","%26")
            }
        }
    }

    // CHANGE FOOTNOTE HREF
    var changeFootnoteHref = function () {
        // split footnotes and citation hrefs
        // citations
        var citations = document.querySelectorAll('a.citation');
        for (var i = 0; i < citations.length; i++) {
            citations[i].href = "#"+citations[i].href.split("#")[1]+"citation"
        }

        citations = document.body.getElementsByClassName('footnotes')
        if (citations.length) {
            citations = citations[0].getElementsByTagName('ol')[0].getElementsByTagName('li')
            for (var i = 0; i < citations.length; i++) {
                if (citations[i].className.includes("citation")) {
                    citations[i].id += "citation"
                }
            }
        }
        // FOR DRAFTS
        citations = document.body.getElementsByClassName('citations')
        if (citations.length) {
            citations = citations[0].getElementsByTagName('ol')[0].getElementsByTagName('li')
            for (var i = 0; i < citations.length; i++) {
                    citations[i].id += "citation"
            }
        }


        // FOOTNOTES
        var footnotes = document.querySelectorAll('a.footnote');
        for (var i = 0; i < footnotes.length; i++) {
            footnotes[i].href = "#"+footnotes[i].href.split("#")[1]+"footnote"
            footnotes[i].id += "footnote"
        }
        var footnotes = document.querySelectorAll('a.reversefootnote');
        for (var i = 0; i < footnotes.length; i++) {
            footnotes[i].href = "#"+footnotes[i].href.split("#")[1]+"footnote"
        }
        // FOR DRAFTS
        footnotes = document.body.getElementsByClassName('footnotes')
        if (footnotes.length) {
            footnotes = footnotes[0].getElementsByTagName('ol')[0].getElementsByTagName('li')
            for (var i = 0; i < footnotes.length; i++) {
                if (!footnotes[i].className.includes("citation")) {
                    footnotes[i].id += "footnote"
                }
                
            }
        }
    }
    
    var additionalResources = function() {
        if (!window.matchMedia("screen").matches) {
            return
        }

        var tags = processHashTags()
        if (tags[0].length) {

            var resourcesContainer = document.createElement('div')
            resourcesContainer.setAttribute('id', 'additional_resources')

            resourcesContainer.innerHTML += `<h1>Additional Resources</h1>

              <h2>Zettelkasten</h2>
              
              <div id="myhashtags">
              </div>
              
              <div id="pinboard_bookmarks">
                <h2>Pinboard Bookmarks</h2>
              </div>

              <div id="devonthink_links">
               <h2>Devonthink</h2>`

            
            document.body.appendChild(resourcesContainer)
            document.getElementById("myhashtags").appendChild(tags[1])
            var links = pinboardLinks(tags[0])
            document.getElementById("pinboard_bookmarks").appendChild(links)
            var dtElement = devonthinkLinks(tags[0]);
            document.getElementById("devonthink_links").appendChild(dtElement)
        }
    }


    
    function handler(event) {

        processMath()
        changeFootnoteHref()
        if (window.matchMedia("screen").matches) {
            bigfootCode()
        }
        setTimeout(function(){relabelFootnotes()}, 2000);
        splitFootnotesAndCitations()
        mermaidCode()
        flowchartCode()
        highlightCode()
        convertLinksToIaLinks()
        additionalResources()

    }

    // WAIT FOR THE 'ia-writer-change' EVENT BEFORE ACTING
    // document.body.addEventListener('ia-writer-change', handler, false);
    if (document.body.hasAttribute('drafts')) {
        // DRAFTS
        $(document).ready(handler);
    } else if (document.body.classList.contains('wy-body-for-nav')) {
        // SPHINX

        $(document).ready(handler);
    } else {
        // iA Writer
        document.body.addEventListener('ia-writer-change', handler, false);
    }
})
