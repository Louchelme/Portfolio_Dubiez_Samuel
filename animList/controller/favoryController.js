/**
 * Objet constant représentant la vue.
 */
const view = {

    //div contenant les cards
    listAnime: document.querySelector('#listAnime'),
    
};

function initPage() {
    view.listAnime.innerHTML ="";

    Anime.restoreState()
    let favoris = Anime.getFavoris();

    for(let key in favoris){
        
        let anime = favoris[key];

        let card = create_favory_card(anime);
        view.listAnime.appendChild(card);

        let input = card.querySelector("input");
        input.value = anime.getCurrentEpisode();

        //Enregistre en local l'episode en cour lorsque l'utilisateur le change
        input.addEventListener('change', (evt) => {
            anime.setCurrentEpisode(evt.target.value);
        });

        let btnRemoveFav = card.querySelector(".btn-favory");
        //Remove des favoris
        btnRemoveFav.addEventListener('click', (evt) => {
            Anime.deleteFavori(anime)
            initPage();
        });

        

    }
}

initPage();




