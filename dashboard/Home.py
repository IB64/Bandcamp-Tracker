"""Home page for the StreamLit dashboard, this is the first page you see."""
import streamlit as st


def main():
    """Main function to create the Home page for the dashboard."""
    st.set_page_config(
        page_title="BandCamp Analytics",
        page_icon="🎵",)

    st.write("# BandCamp Analytics")

    with st.container(border=True):

        st.markdown("""
            Welcome to BandCamp Analytics, a place to explore and discover new music. 
            Whether you're a music enthusiast, searching for upcoming artists, or someone with a keen interest in the ever-evolving music landscape, 
            you're in the right place.
                    
            ## What We Offer
            - **Live Analytics Dashboard:**
            Dive into real-time insights and trends within the music industry through our Live Analytics Dashboard.

            - **Daily Newsletter:**
            Subscribe to our Daily Newsletter and receive music highlights directly in your inbox.

            - **Access to Past Data:**
            Explore the evolution of past music trends. Whether you're conducting research or simply curious about the trajectory of your favorite artists, 
            our Past Data section is a treasure trove of musical insights.

            BandCamp Analytics is more than a platform; it's a symphony of discovery, a community that celebrates the artistry of sound. 
            Join us as we embark on a melodious journey where every beat tells a story. 
            Turn up the volume and start exploring!""")


if __name__ == "__main__":
    main()
