const form = document.querySelector('#video-form');
const videoDiv = document.querySelector('#video-player');
const videoScreen = document.querySelector('#video-screen');

const queryParams = Object.fromEntries(new URLSearchParams(window.location.search));

fetch('/media/all')
    .then(result => result.json())
    .then(result => {

        const myVids = document.querySelector('#your-videos');
        if(result.length > 0){
            for(let vid of result){
                const li = document.createElement('LI');
                const link = document.createElement('A');
                link.innerText = vid;
                link.href = window.location.origin + window.location.pathname + '?video=' + vid;
                li.appendChild(link);
                myVids.appendChild(li);
            }
        }else{
            myVids.innerHTML = 'No videos found';
        }

    });

if(queryParams.video){

    videoScreen.src = `/media/${queryParams.video}`;
    videoDiv.style.display = 'block';
    document.querySelector('#now-playing')
        .innerText = 'Now playing ' + queryParams.video;

}

form.addEventListener('submit', ev => {
    ev.preventDefault();
    let data = new FormData(form);
    let before = performance.now()
    fetch('/media', {
        method: 'POST',
        body: data
    }).then(result => result.text()).then(_ => {
        let after = performance.now();
        window.location.reload();
        console.log('Time taken: ' + (after - before));
    });

});
