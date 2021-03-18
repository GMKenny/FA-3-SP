from tkinter import *
from recommender_system import *
from PIL import Image, ImageTk
from recommender_system import *


class SteamGui:
    def __init__(self, parent, width, height):
        self.parent = parent
        self.frame_dict = {}
        self.db = None
        self.cursor = None
        self.image_list = {1: 'background_recom.png'}
        self.image_resized = {}

        self.resize_image(self.image_list[1], width, height, 1)

        # Begin frame "Start Screen"
        self.begin_frame = Frame(parent, background="grey")
        self.begin_frame.place(relx=0.5, rely=0.5, relwidth=1, relheight=1, anchor='c')
        self.begin_background = Label(self.begin_frame, image= self.image_resized[1])
        self.begin_background.pack(pady=10, padx=10, anchor=NW)

        self.begin_frame_button = Button(self.begin_frame, text="Start", command=lambda: self.switch_screen(1),
                            bg='#0063D3', activebackground='#004BA0', fg='white')
        self.begin_frame_button.place(relx=0.5, rely=0.5, relwidth=0.30, relheight=0.05, anchor='n')


        # Second frame "Main Menu"
        self.second_frame = Frame(parent, background="grey")
        self.second_frame.place(relx=0.5, rely=0.5, relwidth=1, relheight=1, anchor='c')

        self.second_frame_background = Label(self.second_frame, image=self.image_resized[1])
        self.second_frame_background.pack(pady=10, padx=10, anchor=NW)

        self.second_frame_label = Label(self.second_frame, text="Reload Recommendation Menu")
        self.second_frame_label.place(relx=0.5, rely=0.2, relwidth=0.3, relheight=0.05, anchor='n')

        self.begin_frame_button_one = Button(self.second_frame, text="Content order ", command=lambda: recommendation_order_category_process(),
                                         bg='#0063D3', activebackground='#004BA0', fg='white')
        self.begin_frame_button_one.place(relx=0.5, rely=0.3, relwidth=0.30, relheight=0.05, anchor='n')

        self.begin_frame_button_two = Button(self.second_frame, text="Content events", command=lambda: recommendation_events_category_process(),
                                         bg='#0063D3', activebackground='#004BA0', fg='white')
        self.begin_frame_button_two.place(relx=0.5, rely=0.4, relwidth=0.30, relheight=0.05, anchor='n')

        self.begin_frame_button_three = Button(self.second_frame, text="Collaborative orders", command=lambda: recommendation_profile_events_process(),
                                         bg='#0063D3', activebackground='#004BA0', fg='white')
        self.begin_frame_button_three.place(relx=0.5, rely=0.5, relwidth=0.30, relheight=0.05, anchor='n')

        self.begin_frame_button_four = Button(self.second_frame, text="Collaborative events", command=lambda: recommendation_profile_orders_process(),
                                         bg='#0063D3', activebackground='#004BA0', fg='white')
        self.begin_frame_button_four.place(relx=0.5, rely=0.6, relwidth=0.30, relheight=0.05, anchor='n')

        self.second_frame_button_two = Button(self.second_frame, text="Quit",command=lambda: self.quit_program(),
                                          bg='#0063D3', activebackground='#004BA0', fg='white')
        self.second_frame_button_two.place(relx=0.5, rely=0.7, relwidth=0.30, relheight=0.05, anchor='n')

        self.frame_dict.update({0: self.begin_frame, 1: self.second_frame})
        self.frame_dict[0].lift()


    def switch_screen(self, num):
        self.frame_dict[num].lift()

    def quit_program(self):
        try:
            sql_closer(self.db, self.cursor)
        except AttributeError:
            pass
        self.parent.destroy()


    def resize_image(self, item, n_width, n_height, num):
        open_image = Image.open(("./" + item))
        resized_image = open_image.resize((n_width, n_height), Image.ANTIALIAS)
        complete_image = ImageTk.PhotoImage(resized_image)
        self.image_resized.update({num: complete_image})


root = Tk()
root.title("Recommendation system")
screen_width = int(root.winfo_screenwidth() / 2)
screen_height = int(root.winfo_screenheight() / 1.2)
root.geometry("%dx%d+0+0" % (screen_width, screen_height))
steam_gui = SteamGui(root, screen_width, screen_height)
root.mainloop()
