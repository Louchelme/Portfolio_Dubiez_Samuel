/**
 * Objet constant représentant la vue.
 */
const view = {

    //div contenant les cards
    listAnime: document.querySelector('#listAnime'),
    
};


//test des cards
Anime.restoreState()
var favoris = Anime.getFavoris();
console.log('salut');

for(var key in favoris){
    var card = create_favory_card(favoris[key]);
    view.listAnime.appendChild(card); 
    console.log('anim : ' + key);
}


// fetch(`https://api.allorigins.win/get?url=${encodeURIComponent('http://api.jikan.moe/v4/anime?q=s&sfw')}`) //TODO : a foutre dans un DAO api
// .then( async( response ) => {

//     var recherche = await response.json();
//     var data = JSON.parse(recherche.contents).data ;

//     data.forEach( elem => {
//         var anime = new Anime(elem)

//         var card = create_favory_card(anime);
//         view.listAnime.appendChild(card); 

//     });
  
//   })
